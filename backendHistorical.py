import requests
import sqlite3
import pandas as pd
from datetime import datetime,timedelta,date
import os
import time
import logging
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values


ACCESS_TOKEN = 'OUOFRjuCoP5C3uSBni56tOdaUhOG'
INTERVAL = "daily"
LOG_FILE_ADDRESS = os.path.join(os.getcwd(),'tradier_log.txt')
PG_DSN_HIST = os.environ["PG_DSN_HIST"]

def get_engine():
    return create_engine(PG_DSN_HIST, pool_pre_ping=True)

def ensure_schema(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS option_history_eod (
        symbol         TEXT NOT NULL,
        quoteDate      DATE NOT NULL,
        underlyingLast NUMERIC,
        expireDate     DATE NOT NULL,

        callVolume     BIGINT,
        callOpen       NUMERIC,
        callHigh       NUMERIC,
        callLow        NUMERIC,
        callClose      NUMERIC,

        strike         NUMERIC NOT NULL,

        putClose       NUMERIC,
        putLow         NUMERIC,
        putHigh        NUMERIC,
        putOpen        NUMERIC,
        putVolume      BIGINT,

        itmPercCalls   NUMERIC,
        itmPercPuts    NUMERIC,
        dte            INTEGER,

        PRIMARY KEY (symbol, quoteDate, expireDate, strike)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def upsert_rows(engine, rows):
    if not rows:
        return
    cols = list(rows[0].keys())
    tuples = [tuple(r.get(c) for c in cols) for r in rows]
    sql = f"""
    INSERT INTO option_history_eod ({",".join(cols)})
    VALUES %s
    ON CONFLICT (symbol, quoteDate, expireDate, strike)
    DO UPDATE SET
        underlyingLast=COALESCE(EXCLUDED.underlyingLast, option_history_eod.underlyingLast),
        callVolume=COALESCE(EXCLUDED.callVolume, option_history_eod.callVolume),
        callOpen=COALESCE(EXCLUDED.callOpen, option_history_eod.callOpen),
        callHigh=COALESCE(EXCLUDED.callHigh, option_history_eod.callHigh),
        callLow=COALESCE(EXCLUDED.callLow, option_history_eod.callLow),
        callClose=COALESCE(EXCLUDED.callClose, option_history_eod.callClose),
        putClose=COALESCE(EXCLUDED.putClose, option_history_eod.putClose),
        putLow=COALESCE(EXCLUDED.putLow, option_history_eod.putLow),
        putHigh=COALESCE(EXCLUDED.putHigh, option_history_eod.putHigh),
        putOpen=COALESCE(EXCLUDED.putOpen, option_history_eod.putOpen),
        putVolume=COALESCE(EXCLUDED.putVolume, option_history_eod.putVolume),
        itmPercCalls=EXCLUDED.itmPercCalls,
        itmPercPuts=EXCLUDED.itmPercPuts,
        dte=EXCLUDED.dte;
    """
    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            execute_values(cur, sql, tuples, page_size=2000)

def connectToTradierHistory(ticker,startDate,endDate):
    response = requests.get('https://api.tradier.com/v1/markets/history',
    params={'symbol': ticker, 'interval': INTERVAL, 'start': startDate, 'end': endDate},
    headers = {'Authorization': 'Bearer {}'.format(ACCESS_TOKEN), 'Accept': 'application/json'}
    )
    if response.status_code != 200:
        return None
    json_response = response.json()
    if 'history' in json_response and json_response['history'] is not None:
        return json_response['history']
    else:
        return None

def getStockCloseOnDate(symbol,d):
    response = requests.get('https://api.tradier.com/v1/markets/history',
    params={'symbol': symbol, 'interval': INTERVAL, 'start': d.strftime('%Y-%m-%d'), 'end': d.strftime('%Y-%m-%d')},
    headers = {'Authorization': 'Bearer {}'.format(ACCESS_TOKEN), 'Accept': 'application/json'}
    )
    json_response = response.json()

    if 'history' in json_response and json_response['history'] is not None and 'day' in json_response['history'] and json_response['history']['day'] is not None:
        day = json_response['history']['day']
        if type(day) == list and len(day) > 0 and 'close' in day[0]:
            return float(day[0]['close'])
        if type(day) == dict and 'close' in day:
            return float(day['close'])
    return None

def printAndLog(log_message):
    log_file = LOG_FILE_ADDRESS
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')
    logging.info(log_message)
    print(log_message)

def buildOCC(symbol, exp_date, call_put, strike):
    strike_int = int(strike * 1000)
    return symbol + exp_date.strftime('%y%m%d') + call_put + str(strike_int).zfill(8)

def isTradingDay(d):
    if d.weekday() >= 5:
        return False
    return True

def previousTradingDay(d):
    while not isTradingDay(d):
        d -= timedelta(days=1)
    return d

# creates list of dates that are likely valid expiration dates
def getCandidateExpirations(start_date,end_date,include_intraweek=True):
    expirations = []
    d = start_date
    while d <= end_date:
        if d.weekday() == 4:
            expirations.append(previousTradingDay(d))
        if include_intraweek and (d.weekday() == 0 or d.weekday() == 2):
            expirations.append(previousTradingDay(d))
        d += timedelta(days=1)
    expirations = sorted(list(set(expirations)))
    return expirations

def normalize_days(history_obj):
    if history_obj is None:
        return []
    if 'day' not in history_obj or history_obj['day'] is None:
        return []
    day = history_obj['day']
    if type(day) == dict:
        return [day]
    if type(day) == list:
        return day
    return []

def historyDaysToRows(underlying_symbol,exp_date,strike,call_put,history_obj):
    rows = []
    underlyingLast = 100
    expireDate = exp_date.date()
    itmPercCalls = ((strike - underlyingLast) / underlyingLast) * 100 if underlyingLast not in (None,0) else None
    itmPercPuts = (-itmPercCalls) if itmPercCalls is not None else None
    days = normalize_days(history_obj)
    for d in days:
        if 'date' not in d:
            continue
        quoteDate = date.fromisoformat(d['date'])
        dte = (expireDate - quoteDate).days
        row = {
            "symbol": underlying_symbol,
            "quoteDate": quoteDate,
            "underlyingLast": underlyingLast,
            "expireDate": expireDate,
            "callVolume": None,
            "callOpen": None,
            "callHigh": None,
            "callLow": None,
            "callClose": None,
            "strike": strike,
            "putClose": None,
            "putLow": None,
            "putHigh": None,
            "putOpen": None,
            "putVolume": None,
            "itmPercCalls": itmPercCalls,
            "itmPercPuts": itmPercPuts,
            "dte": dte
        }
        if call_put == 'C':
            row["callOpen"] = d.get("open")
            row["callHigh"] = d.get("high")
            row["callLow"] = d.get("low")
            row["callClose"] = d.get("close")
            row["callVolume"] = d.get("volume")
        if call_put == 'P':
            row["putOpen"] = d.get("open")
            row["putHigh"] = d.get("high")
            row["putLow"] = d.get("low")
            row["putClose"] = d.get("close")
            row["putVolume"] = d.get("volume")
        rows.append(row)
    return rows

# tests 20 likely strikes around the stock price (at expiration date). if none are valid, assumes invalid exp date and skips.
def expirationLooksValid(symbol,exp_date):
    print("checking expiration date:",exp_date.strftime('%Y-%m-%d'))
    stockPriceOnExpDate = getStockCloseOnDate(symbol,exp_date)
    if stockPriceOnExpDate is None:
        print("no stock price found for date, skipping expiration")
        return False
    print("stock price on expiration date:",stockPriceOnExpDate)
    atm = int(round(stockPriceOnExpDate))
    strikes_tested = []
    total = 21
    i = 0
    startDate = (exp_date - timedelta(days=31)).strftime('%Y-%m-%d')
    endDate = exp_date.strftime('%Y-%m-%d')
    for strike in range(atm-10,atm+11):
        i += 1
        strikes_tested.append(strike)
        print(str(i) + "/" + str(total) + " testing strike " + str(strike),end="\r",flush=True)
        occ = buildOCC(symbol,exp_date,'C',strike)
        result = connectToTradierHistory(occ,startDate,endDate)
        if result:
            print(" " * 60,end="\r")
            print("expiration accepted, tested strikes:",strikes_tested)
            return True
        time.sleep(0.25)
    print(" " * 60,end="\r")
    print("expiration rejected, tested strikes:",strikes_tested)
    return False

# --- BEGINNING OF CODE ---
engine = get_engine()
ensure_schema(engine)

ticker = 'AAPL'

start_date = datetime(2025,1,1)
end_date = datetime(2025,1,10)

# creates list of dates that are likely valid expiration dates
expirations = getCandidateExpirations(start_date,end_date,True)

for exp_date in expirations:
    # validate expiration by probing atm strikes
    if not expirationLooksValid(ticker,exp_date):
        printAndLog("NOTHING EXPIRATION " + exp_date.strftime('%Y-%m-%d'))
        continue
    # define history window (1 month before expiration -> expiration)
    startDate = (exp_date - timedelta(days=31)).strftime('%Y-%m-%d')
    endDate = exp_date.strftime('%Y-%m-%d')
    # brute force scan full strike range once expiration is confirmed
    # scans both calls and puts, strikes 1..1000 in $1 increments
    total = 2000
    count = 0
    for call_put in ['C','P']:
        for strike in range(1,1001):
            count += 1
            print(str(count) + "/" + str(total) + " testing " + call_put + " strike " + str(strike),end="\r",flush=True)
            occ = buildOCC(ticker,exp_date,call_put,strike)
            result = connectToTradierHistory(occ,startDate,endDate)
            if result:
                print(" " * 80,end="\r")
                print("success",exp_date.strftime('%Y-%m-%d'),call_put,"strike",strike)
                rows = historyDaysToRows(ticker,exp_date,strike,call_put,result)
                upsert_rows(engine, rows)
            time.sleep(0.8)
