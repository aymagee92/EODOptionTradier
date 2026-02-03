from flask import Flask, request, render_template_string, Response, url_for
from sqlalchemy import create_engine, text
import os
import csv
import io
from decimal import Decimal

from frontendStorage import register_storage_routes
from frontendHistorical import register_historical_routes

app = Flask(__name__, static_folder="static")
engine = create_engine(os.environ["PG_DSN"], pool_pre_ping=True)

# Register shared routes (same app, same port)
register_storage_routes(app, engine)
register_historical_routes(app)

TABLE = "option_chain_eod"

COLUMNS = [
    "symbol",
    "quotedate",
    "underlyinglast",
    "expiredate",
    "callvolume",
    "callbid",
    "callask",
    "callmid",
    "strike",
    "putmid",
    "putbid",
    "putask",
    "putvolume",
    "itmperccalls",
    "itmpercputs",
    "dte",
]

def fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (float, Decimal)):
        return f"{v:.2f}"
    return str(v)

# -------------------------
# Disk status (droplet-wide, OS-level)
# -------------------------
import subprocess

def _df_usage(mount: str):
    try:
        out = subprocess.check_output(["df", "-h", mount], text=True).strip().splitlines()
        if len(out) < 2:
            return (None, None, None)
        parts = out[1].split()
        return (parts[2], parts[1], parts[4])
    except Exception:
        return (None, None, None)

def get_latest_disk_status():
    root_used, root_total, root_pct = _df_usage("/")

    vol_used = vol_total = vol_pct = None
    for mount in ("/mnt/volume", "/volume", "/mnt", "/data"):
        u, t, p = _df_usage(mount)
        if u and t and p:
            vol_used, vol_total, vol_pct = u, t, p
            break

    return {
        "root_used": root_used,
        "root_total": root_total,
        "root_pct": root_pct,
        "vol_used": vol_used,
        "vol_total": vol_total,
        "vol_pct": vol_pct,
    }

HEADER_HTML = """
<div class="header">
  <div class="title">
    <h1>End of Day Option Prices</h1>
    <div class="topnav">
      <a class="tab {% if active_page=='options' %}active{% endif %}" href="{{ url_for('index') }}">
        Option Info
      </a>
      <a class="tab {% if active_page=='historical' %}active{% endif %}" href="/historical">
        Historical Options
      </a>
      <a class="tab {% if active_page=='storage' %}active{% endif %}" href="{{ url_for('storage_dashboard') }}">
        Storage Graph
      </a>
      <a class="tab {% if active_page=='stockdata' %}active{% endif %}" href="/stockdata">
        Stock Data
      </a>
    </div>
  </div>

  <div class="storage">
    <span class="pill"><b>Root</b> <code>{{ disk.root_used or "—" }}</code> / <code>{{ disk.root_total or "—" }}</code> ({{ disk.root_pct or "—" }})</span>
    <span class="pill"><b>Volume</b> <code>{{ disk.vol_used or "—" }}</code> / <code>{{ disk.vol_total or "—" }}</code> ({{ disk.vol_pct or "—" }})</span>
  </div>
</div>
"""

TABLE_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Options</title>
  <link rel="stylesheet" href="{{ url_for('static', filename='options.css') }}">
</head>
<body>
  <div class="container">
    """ + HEADER_HTML + """

    <form id="qform" method="get" class="card">
      <div class="controls">
        <div class="controls-left">
          <div class="field">
            <span>Rows</span>
            <input type="number" name="limit" value="{{ limit }}" min="1" max="50000" />
          </div>

          <button class="btn ghost" type="button" id="clearBtn">✕ Clear all</button>
          <button class="btn ghost" type="button" id="clearSortsBtn">↕ Clear sorts</button>

          <div class="small">Filters & sorts are applied via URL (GET).</div>
        </div>

        <div class="controls-right">
          <button class="btn primary" type="submit" name="format" value="html">Run</button>
          <button class="btn" type="submit" name="format" value="csv">Download CSV</button>
        </div>
      </div>

      <div class="chips" id="chips">
        {% for col in columns %}
          {% if filters.get(col) %}
            <span class="chip filter" data-kind="filter" data-col="{{ col }}">
              Filter <b>{{ col }}</b>: {{ filters.get(col) }}
              <span class="x">×</span>
            </span>
          {% endif %}
          {% if sorts.get(col) %}
            <span class="chip sort" data-kind="sort" data-col="{{ col }}">
              Sort <b>{{ col }}</b>: {{ '▲' if sorts.get(col)=='asc' else '▼' }}
              <span class="x">×</span>
            </span>
          {% endif %}
        {% endfor %}
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              {% for col in columns %}
                {% set s = sorts.get(col, '') %}
                <th data-col="{{ col }}" data-sort="{{ s }}">
                  <div class="th-top">
                    <input type="text" name="f_{{ col }}" value="{{ filters.get(col,'') }}" placeholder="Filter…" />
                  </div>
                  <input type="hidden" name="s_{{ col }}" value="{{ s }}">
                  <div class="th-bottom" data-action="sort">
                    <span>{{ col }}</span>
                    <span class="sort-ind">
                      {% if s=='asc' %}▲{% elif s=='desc' %}▼{% else %}↕{% endif %}
                    </span>
                  </div>
                </th>
              {% endfor %}
            </tr>
          </thead>

          <tbody>
            {% for row in rows %}
              <tr>
                {% for col in columns %}
                  <td class="{% if col=='symbol' %}mono{% endif %}">
                    {{ fmt(row.get(col)) }}
                  </td>
                {% endfor %}
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div class="footer-note">
        Showing {{ rows|length }} row(s). Scroll horizontally for more columns.
      </div>
    </form>
  </div>

  <script src="{{ url_for('static', filename='options.js') }}"></script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def index():
    filters = {}
    sorts = {}
    where_clauses = []
    params = {}

    for col in COLUMNS:
        fval = request.args.get(f"f_{col}")
        sval = request.args.get(f"s_{col}")

        if fval:
            filters[col] = fval
            where_clauses.append(f"{col}::text ILIKE :f_{col}")
            params[f"f_{col}"] = f"%{fval}%"

        if sval in ("asc", "desc"):
            sorts[col] = sval

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    order_sql = (
        "ORDER BY " + ", ".join(f"{c} {d}" for c, d in sorts.items())
        if sorts
        else "ORDER BY quotedate DESC, symbol ASC, expiredate ASC, strike ASC"
    )

    limit = int(request.args.get("limit", 100))
    params["limit"] = limit

    sql = f"""
    SELECT {",".join(COLUMNS)}
    FROM {TABLE}
    {where_sql}
    {order_sql}
    LIMIT :limit
    """

    with engine.connect() as conn:
        rows = [dict(r._mapping) for r in conn.execute(text(sql), params)]

    if request.args.get("format") == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=options_export.csv"},
        )

    return render_template_string(
        TABLE_PAGE,
        active_page="options",
        rows=rows,
        columns=COLUMNS,
        filters=filters,
        sorts=sorts,
        limit=limit,
        fmt=fmt,
        disk=get_latest_disk_status(),
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)



