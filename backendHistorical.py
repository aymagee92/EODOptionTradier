import requests
from datetime import datetime, timedelta, date
import os
import time
import logging
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

ACCESS_TOKEN = 'OUOFRjuCoP5C3uSBni56tOdaUhOG'
INTERVAL = "daily"
LOG_FILE_ADDRESS = os.path.join(os.getcwd(), 'tradier_log.txt')
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
        itmPercCalls=COALESCE(EXCLUDED.itmPercCalls, option_history_eod.itmPercCalls),
        itmPercPuts=COALESCE(EXCLUDED.itmPercPuts, option_history_eod.itmPercPuts),
        dte=COALESCE(EXCLUDED.dte, option_history_eod.dte);
    """
    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            execute_values(cur, sql, tuples, page_size=2000)

def connectToTradierHistory(ticker, startDate, endDate):
    response = requests.get(
        'https://api.tradier.com/v1/markets/history',
        params={'symbol': ticker, 'interval': INTERVAL, 'start': startDate, 'end': endDate},
        headers={'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'application/json'}
    )
    if response.status_code != 200:
        return None
    json_response = response.json()
    if 'history' in json_response and json_response['history'] is not None:
        return json_response['history']
    return None

def getStockCloseOnDate(symbol, d):
    response = requests.get(
        'https://api.tradier.com/v1/markets/history',
        params={'symbol': symbol, 'interval': INTERVAL,
                'start': d.strftime('%Y-%m-%d'), 'end': d.strftime('%Y-%m-%d')},
        headers={'Authorization': f'Bearer {ACCESS_TOKEN}', 'Accept': 'application/json'}
    )
    json_response = response.json()

    if ('history' in json_response and json_response['history'] is not None and
        'day' in json_response['history'] and json_response['history']['day'] is not None):
        day = json_response['history']['day']
        if isinstance(day, list) and len(day) > 0 and 'close' in day[0]:
            return float(day[0]['close'])
        if isinstance(day, dict) and 'close' in day:
            return float(day['close'])
    return None

def printAndLog(log_message):
    logging.basicConfig(filename=LOG_FILE_ADDRESS, level=logging.INFO,
                        format='%(asctime)s - %(message)s')
    logging.info(log_message)
    print(log_message)

def buildOCC(symbol, exp_date, call_put, strike):
    strike_int = int(strike * 1000)
    return symbol + exp_date.strftime('%y%m%d') + call_put + str(strike_int).zfill(8)

def isTradingDay(d):
    return d.weekday() < 5

def previousTradingDay(d):
    while not isTradingDay(d):
        d -= timedelta(days=1)
    return d

def getCandidateExpirations(start_date, end_date, include_intraweek=True):
    expirations = []
    d = start_date
    while d <= end_date:
        if d.weekday() == 4:
            expirations.append(previousTradingDay(d))
        if include_intraweek and (d.weekday() == 0 or d.weekday() == 2):
            expirations.append(previousTradingDay(d))
        d += timedelta(days=1)
    return sorted(list(set(expirations)))

def normalize_days(history_obj):
    if history_obj is None:
        return []
    if 'day' not in history_obj or history_obj['day'] is None:
        return []
    day = history_obj['day']
    if isinstance(day, dict):
        return [day]
    if isinstance(day, list):
        return day
    return []

def historyDaysToRows(underlying_symbol, exp_date, strike, call_put, history_obj):
    """
    - underlyingLast is left as None for now.
    - We fill underlyingLast later, AFTER all data is stored, based on quoteDate.
    """
    rows = []
    expireDate = exp_date.date()
    days = normalize_days(history_obj)

    for d in days:
        if 'date' not in d:
            continue

        quoteDate = date.fromisoformat(d['date'])
        dte = (expireDate - quoteDate).days

        row = {
            "symbol": underlying_symbol,
            "quoteDate": quoteDate,
            "underlyingLast": None,
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

            "itmPercCalls": None,
            "itmPercPuts": None,
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

def expirationLooksValid(symbol, exp_date):
    print("checking expiration date:", exp_date.strftime('%Y-%m-%d'))
    stockPriceOnExpDate = getStockCloseOnDate(symbol, exp_date)
    if stockPriceOnExpDate is None:
        print("no stock price found for date, skipping expiration")
        return False

    print("stock price on expiration date:", stockPriceOnExpDate)
    atm = int(round(stockPriceOnExpDate))

    strikes_tested = []
    total = 21
    i = 0
    startDate = (exp_date - timedelta(days=31)).strftime('%Y-%m-%d')
    endDate = exp_date.strftime('%Y-%m-%d')

    for strike in range(atm - 10, atm + 11):
        i += 1
        strikes_tested.append(strike)
        print(f"{i}/{total} testing strike {strike}", end="\r", flush=True)

        occ = buildOCC(symbol, exp_date, 'C', strike)
        result = connectToTradierHistory(occ, startDate, endDate)
        if result:
            print(" " * 60, end="\r")
            print("expiration accepted, tested strikes:", strikes_tested)
            return True

        time.sleep(0.25)

    print(" " * 60, end="\r")
    print("expiration rejected, tested strikes:", strikes_tested)
    return False

# ----------------------------
# NEW: reject duplicate tickers (run historical once per ticker)
# ----------------------------
def ticker_already_loaded(engine, symbol: str) -> bool:
    sql = "SELECT 1 FROM option_history_eod WHERE symbol = :sym LIMIT 1"
    with engine.connect() as conn:
        return conn.execute(text(sql), {"sym": symbol}).fetchone() is not None

# ----------------------------
# After-the-fact filling of underlyingLast
# ----------------------------
def get_quote_date_range(engine, symbol: str):
    sql = """
    SELECT MIN(quoteDate) AS min_qd, MAX(quoteDate) AS max_qd
    FROM option_history_eod
    WHERE symbol = :sym
    """
    with engine.connect() as conn:
        r = conn.execute(text(sql), {"sym": symbol}).fetchone()
        if not r or r[0] is None or r[1] is None:
            return None, None
        return r[0], r[1]

def get_underlying_close_map_for_range(symbol: str, start_d: date, end_d: date) -> dict:
    history_obj = connectToTradierHistory(
        symbol,
        start_d.strftime("%Y-%m-%d"),
        end_d.strftime("%Y-%m-%d"),
    )
    close_map = {}
    for day in normalize_days(history_obj):
        ds = day.get("date")
        if not ds:
            continue
        qd = date.fromisoformat(ds)
        c = day.get("close")
        if c is None:
            continue
        try:
            close_map[qd] = float(c)
        except Exception:
            continue
    return close_map

def update_underlying_last(engine, symbol: str, close_map: dict):
    if not close_map:
        return

    triples = [(symbol, qd, close) for qd, close in close_map.items()]

    sql = """
    UPDATE option_history_eod t
    SET underlyingLast = v.price
    FROM (VALUES %s) AS v(symbol, quoteDate, price)
    WHERE t.symbol = v.symbol
      AND t.quoteDate = v.quoteDate
      AND t.underlyingLast IS NULL;
    """

    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            execute_values(
                cur,
                sql,
                triples,
                template="(%s::text, %s::date, %s::numeric)",
                page_size=2000
            )

# --- BEGINNING OF CODE ---
engine = get_engine()
ensure_schema(engine)

ticker = 'AAPL'

# NEW: reject duplicate tickers BEFORE doing any API work
if ticker_already_loaded(engine, ticker):
    print(f"[SKIP] {ticker} already exists in option_history_eod. Exiting to avoid duplicate run.")
    raise SystemExit(0)

start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 10)

expirations = getCandidateExpirations(start_date, end_date, True)

for exp_date in expirations:
    if not expirationLooksValid(ticker, exp_date):
        printAndLog("NOTHING EXPIRATION " + exp_date.strftime('%Y-%m-%d'))
        continue

    startDate = (exp_date - timedelta(days=31)).strftime('%Y-%m-%d')
    endDate = exp_date.strftime('%Y-%m-%d')

    total = 2000
    count = 0
    for call_put in ['C', 'P']:
        for strike in range(1, 1001):
            count += 1
            print(f"{count}/{total} testing {call_put} strike {strike}", end="\r", flush=True)

            occ = buildOCC(ticker, exp_date, call_put, strike)
            result = connectToTradierHistory(occ, startDate, endDate)

            if result:
                print(" " * 80, end="\r")
                print("success", exp_date.strftime('%Y-%m-%d'), call_put, "strike", strike)

                rows = historyDaysToRows(ticker, exp_date, strike, call_put, result)
                upsert_rows(engine, rows)

            time.sleep(0.8)

# After all option rows are stored, fill underlyingLast based on quoteDate.
min_qd, max_qd = get_quote_date_range(engine, ticker)
if min_qd and max_qd:
    print(f"Filling underlyingLast for {ticker}: {min_qd} -> {max_qd}")
    close_map = get_underlying_close_map_for_range(ticker, min_qd, max_qd)
    update_underlying_last(engine, ticker, close_map)
    print("Done updating underlyingLast.")
else:
    print("No rows found; nothing to update.")


