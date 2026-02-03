import os
import time
import logging
import random
import requests
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("EOD")

# ----------------------------
# CONFIG
# ----------------------------
# Renamed from TICKERS -> frontendTickers and made env-overridable.
# Set like: export FRONTEND_TICKERS="AAPL,MSFT,TSLA,AMD"
frontendTickers = [
    t.strip()
    for t in os.environ.get("FRONTEND_TICKERS", "AAPL,MSFT,TSLA,AMD").split(",")
    if t.strip()
]

TRADIER_TOKEN = os.environ["TRADIER_ACCESS_TOKEN"]
PG_DSN = os.environ["PG_DSN"]

BASE_URL = "https://api.tradier.com/v1"

# Performance knobs
HTTP_TIMEOUT = 20
MAX_RETRIES = 5
TICKERS_PER_BATCH = 10
SLEEP_BETWEEN_TICKERS_SECONDS = 0.0      # set >0 if you hit rate limits
SLEEP_BETWEEN_EXPIRATIONS_SECONDS = 0.0  # set >0 if you hit rate limits

HEADERS = {
    "Authorization": f"Bearer {TRADIER_TOKEN}",
    "Accept": "application/json",
}

session = requests.Session()
session.headers.update(HEADERS)

# ----------------------------
# HTTP HELPERS (fast + resilient)
# ----------------------------
def tradier_get(path: str, params: dict | None = None) -> dict:
    url = f"{BASE_URL}{path}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=HTTP_TIMEOUT)

            if r.status_code == 429:
                sleep_s = min(30, (2 ** attempt)) + random.random()
                log.warning(
                    "429 rate limit on %s. sleep %.1fs (attempt %s/%s)",
                    path, sleep_s, attempt, MAX_RETRIES
                )
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                log.error(
                    "Tradier error %s %s params=%s body=%s",
                    r.status_code, url, params, r.text[:800]
                )
                r.raise_for_status()

            return r.json()

        except requests.RequestException as e:
            sleep_s = min(30, (2 ** attempt)) + random.random()
            log.warning(
                "HTTP error on %s attempt %s/%s: %s. sleep %.1fs",
                path, attempt, MAX_RETRIES, e, sleep_s
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Tradier GET failed after {MAX_RETRIES} retries: {url} params={params}")

# ----------------------------
# TRADIER ENDPOINTS
# ----------------------------
def get_underlying_last(ticker: str) -> float | None:
    j = tradier_get("/markets/quotes", {"symbols": ticker, "greeks": "false"})
    q = (j.get("quotes") or {}).get("quote")
    if isinstance(q, list) and q:
        q = q[0]
    if isinstance(q, dict):
        return q.get("last")
    return None

def get_expirations(ticker: str) -> list[str]:
    j = tradier_get(
        "/markets/options/expirations",
        {"symbol": ticker, "includeAllRoots": "true", "strikes": "false"}
    )
    dates = (j.get("expirations") or {}).get("date", [])
    if isinstance(dates, str):
        return [dates]
    return dates or []

def get_chain(ticker: str, expiration: str) -> list[dict]:
    j = tradier_get("/markets/options/chains", {"symbol": ticker, "expiration": expiration, "greeks": "false"})
    options = (j.get("options") or {}).get("option", [])
    if isinstance(options, dict):
        return [options]
    return options or []

# ----------------------------
# DB
# ----------------------------
def get_engine():
    return create_engine(PG_DSN, pool_pre_ping=True)

def _get_primary_key_cols(conn, table_name: str) -> list[str]:
    sql = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = :t
      AND tc.table_schema = 'public'
    ORDER BY kcu.ordinal_position;
    """
    return [r[0] for r in conn.execute(text(sql), {"t": table_name}).fetchall()]

def _get_pk_constraint_name(conn, table_name: str) -> str | None:
    sql = """
    SELECT tc.constraint_name
    FROM information_schema.table_constraints tc
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = :t
      AND tc.table_schema = 'public'
    LIMIT 1;
    """
    r = conn.execute(text(sql), {"t": table_name}).fetchone()
    return r[0] if r else None

def ensure_schema(engine):
    # Create table with new schema (including runTime) if it doesn't exist
    ddl = """
    CREATE TABLE IF NOT EXISTS option_chain_eod (
        symbol           TEXT NOT NULL,
        quoteDate        DATE NOT NULL,
        runTime          TIME NOT NULL,
        underlyingLast   NUMERIC,
        expireDate       DATE NOT NULL,
        strike           NUMERIC NOT NULL,

        callSymbol       TEXT,
        callVolume       BIGINT,
        callBid          NUMERIC,
        callAsk          NUMERIC,
        callMid          NUMERIC,

        putSymbol        TEXT,
        putVolume        BIGINT,
        putBid           NUMERIC,
        putAsk           NUMERIC,
        putMid           NUMERIC,

        itmPercCalls     NUMERIC,
        itmPercPuts      NUMERIC,
        dte              INTEGER,

        PRIMARY KEY (quoteDate, runTime, symbol, expireDate, strike)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

        # If the table existed before (without runTime), migrate it safely
        conn.execute(text("ALTER TABLE option_chain_eod ADD COLUMN IF NOT EXISTS runTime TIME;"))

        # Backfill any NULL runTime for legacy rows so we can enforce NOT NULL
        conn.execute(text("UPDATE option_chain_eod SET runTime = COALESCE(runTime, TIME '00:00:00') WHERE runTime IS NULL;"))
        conn.execute(text("ALTER TABLE option_chain_eod ALTER COLUMN runTime SET NOT NULL;"))

        wanted_pk = ["quotedate", "runtime", "symbol", "expiredate", "strike"]
        pk_cols = [c.lower() for c in _get_primary_key_cols(conn, "option_chain_eod")]

        if pk_cols and pk_cols != wanted_pk:
            pk_name = _get_pk_constraint_name(conn, "option_chain_eod")
            if pk_name:
                conn.execute(text(f'ALTER TABLE option_chain_eod DROP CONSTRAINT "{pk_name}";'))
            conn.execute(text("ALTER TABLE option_chain_eod ADD PRIMARY KEY (quoteDate, runTime, symbol, expireDate, strike);"))

def upsert_rows(engine, rows: list[dict]):
    if not rows:
        return

    cols = list(rows[0].keys())
    tuples = [tuple(r.get(c) for c in cols) for r in rows]

    conflict_cols = ("quoteDate", "runTime", "symbol", "expireDate", "strike")
    update_cols = [c for c in cols if c not in conflict_cols]

    sql = f"""
    INSERT INTO option_chain_eod ({",".join(cols)})
    VALUES %s
    ON CONFLICT ({",".join(conflict_cols)})
    DO UPDATE SET
    """ + ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)

    with engine.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            execute_values(cur, sql, tuples, page_size=2000)

# ----------------------------
# UTIL
# ----------------------------
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def current_runtime_hhmmss() -> str:
    # Store the actual time the job ran (ET), since you're scheduling at 09:30 and 16:00
    return datetime.now(ZoneInfo("America/New_York")).strftime("%H:%M:%S")

# ----------------------------
# MAIN
# ----------------------------
def run_eod():
    engine = get_engine()
    ensure_schema(engine)

    run_date = date.today()
    run_time = current_runtime_hhmmss()
    log.info(
        "RUN START | %s tickers | run_date=%s run_time=%s",
        len(frontendTickers),
        run_date.isoformat(),
        run_time
    )

    for tickers_batch in chunked(frontendTickers, TICKERS_PER_BATCH):
        for ticker in tickers_batch:
            t0 = time.time()
            try:
                underlying_last = get_underlying_last(ticker)
                if underlying_last is None:
                    log.warning("%s: underlying last is None (quotes). Continuing.", ticker)

                expirations = get_expirations(ticker)
                if not expirations:
                    log.warning("%s: no expirations", ticker)
                    continue

                total_rows = 0

                for exp in expirations:
                    chain = get_chain(ticker, exp)
                    if not chain:
                        continue

                    exp_date = date.fromisoformat(exp)
                    dte = (exp_date - run_date).days

                    calls = {}
                    puts = {}

                    for o in chain:
                        s = o.get("strike")
                        if s is None:
                            continue
                        strike_i = int(round(float(s) * 1000))

                        ot = o.get("option_type")
                        if ot == "call":
                            calls[strike_i] = o
                        elif ot == "put":
                            puts[strike_i] = o

                    strikes_i = set(calls) | set(puts)
                    if not strikes_i:
                        continue

                    rows = []
                    for strike_i in strikes_i:
                        strike = strike_i / 1000.0
                        call = calls.get(strike_i, {}) or {}
                        put = puts.get(strike_i, {}) or {}

                        callBid = call.get("bid")
                        callAsk = call.get("ask")
                        putBid = put.get("bid")
                        putAsk = put.get("ask")

                        callMid = (callBid + callAsk) / 2 if callBid is not None and callAsk is not None else None
                        putMid = (putBid + putAsk) / 2 if putBid is not None and putAsk is not None else None

                        itmPercCalls = (
                            ((underlying_last - strike) / strike) * 100
                            if underlying_last not in (None, 0) and strike not in (None, 0)
                            else None
                        )
                        itmPercPuts = (-itmPercCalls) if itmPercCalls is not None else None

                        rows.append({
                            "symbol": ticker,
                            "quoteDate": run_date,
                            "runTime": run_time,
                            "underlyingLast": underlying_last,
                            "expireDate": exp_date,
                            "strike": strike,

                            "callSymbol": call.get("symbol"),
                            "callVolume": call.get("volume"),
                            "callBid": callBid,
                            "callAsk": callAsk,
                            "callMid": callMid,

                            "putSymbol": put.get("symbol"),
                            "putVolume": put.get("volume"),
                            "putBid": putBid,
                            "putAsk": putAsk,
                            "putMid": putMid,

                            "itmPercCalls": itmPercCalls,
                            "itmPercPuts": itmPercPuts,
                            "dte": dte,
                        })

                    upsert_rows(engine, rows)
                    total_rows += len(rows)

                    if SLEEP_BETWEEN_EXPIRATIONS_SECONDS > 0:
                        time.sleep(SLEEP_BETWEEN_EXPIRATIONS_SECONDS)

                dt = time.time() - t0
                log.info("%s: saved %s rows in %.2fs", ticker, total_rows, dt)

            except Exception as e:
                log.exception("%s: failed: %s", ticker, e)

            if SLEEP_BETWEEN_TICKERS_SECONDS > 0:
                time.sleep(SLEEP_BETWEEN_TICKERS_SECONDS)

    log.info("RUN COMPLETE | run_date=%s run_time=%s", run_date.isoformat(), run_time)

if __name__ == "__main__":
    run_eod()
