from flask import request, render_template_string, Response, url_for
from sqlalchemy import create_engine, text
import os
import csv
import io
from decimal import Decimal
import subprocess

from frontendStorage import register_storage_routes

# -------------------------
# CONFIG
# -------------------------
TABLE = "option_history_eod"

COLUMNS = [
    "symbol",
    "quotedate",
    "underlyinglast",
    "expiredate",
    "callvolume",
    "callopen",
    "callhigh",
    "calllow",
    "callclose",
    "strike",
    "putclose",
    "putlow",
    "puthigh",
    "putopen",
    "putvolume",
    "itmperccalls",
    "itmpercputs",
    "dte",
]

engine_hist = create_engine(os.environ["PG_DSN_HIST"], pool_pre_ping=True)

# -------------------------
# FORMATTERS
# -------------------------
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
# DISK STATUS (OS-level)
# -------------------------
def _df_usage(mount: str):
    try:
        out = subprocess.check_output(["df", "-h", mount], text=True).strip().splitlines()
        if len(out) < 2:
            return (None, None, None)
        parts = out[1].split()
        return parts[2], parts[1], parts[4]  # used, total, pct
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

# -------------------------
# HEADER
# -------------------------
HEADER_HTML = """
<div class="header">
  <div class="topnav">
    <a class="tab" href="/">Option Info</a>
    <a class="tab active" href="/historical">Historical Options</a>
    <a class="tab" href="{{ url_for('storage_dashboard') }}">Storage Graph</a>
    <a class="tab" href="/stockdata">Stock Data</a>
  </div>

  <div class="storage">
    <span class="pill"><b>Root</b>
      <code>{{ disk.root_used or "—" }}</code> /
      <code>{{ disk.root_total or "—" }}</code>
      ({{ disk.root_pct or "—" }})
    </span>
    <span class="pill"><b>Volume</b>
      <code>{{ disk.vol_used or "—" }}</code> /
      <code>{{ disk.vol_total or "—" }}</code>
      ({{ disk.vol_pct or "—" }})
    </span>
  </div>
</div>
"""

# -------------------------
# PAGE TEMPLATE
# -------------------------
TABLE_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Historical Options</title>
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
        </div>

        <div class="controls-right">
          <button class="btn primary" type="submit">Run</button>
          <button class="btn" type="submit" name="format" value="csv">Download CSV</button>
        </div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              {% for col in columns %}
                <th>{{ col }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% for row in rows %}
              <tr>
                {% for col in columns %}
                  <td>{{ fmt(row.get(col)) }}</td>
                {% endfor %}
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div class="footer-note">
        Showing {{ rows|length }} row(s).
      </div>
    </form>
  </div>

  <script src="{{ url_for('static', filename='options.js') }}"></script>
</body>
</html>
"""

# -------------------------
# ROUTE REGISTRATION
# -------------------------
def register_historical_routes(app):
    register_storage_routes(app, engine_hist)

    @app.route("/historical", methods=["GET"])
    def historical():
        limit = int(request.args.get("limit", 100))

        sql = f"""
        SELECT {",".join(COLUMNS)}
        FROM {TABLE}
        ORDER BY quotedate DESC, symbol ASC, expiredate ASC, strike ASC
        LIMIT :limit
        """

        with engine_hist.connect() as conn:
            rows = [dict(r._mapping) for r in conn.execute(text(sql), {"limit": limit})]

        if request.args.get("format") == "csv":
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": "attachment; filename=historical_options_export.csv"},
            )

        return render_template_string(
            TABLE_PAGE,
            rows=rows,
            columns=COLUMNS,
            limit=limit,
            fmt=fmt,
            disk=get_latest_disk_status(),
        )


