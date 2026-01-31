from flask import Flask, request, render_template_string, Response, url_for
from sqlalchemy import create_engine, text
import os
import csv
import io
from decimal import Decimal

app = Flask(__name__, static_folder="static")
engine = create_engine(os.environ["PG_DSN_HIST"], pool_pre_ping=True)

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

HEADER_HTML = """
<div class="header">
  <div class="title">
    <h1>Historical Option Prices</h1>
  </div>
</div>
"""

TABLE_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Historical Options</title>
  <link rel="stylesheet" href="{{ url_for('static', filename='historical.css') }}">
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

          <button class="btn ghost" type="button" id="clearBtn" title="Clear filters & sorts">
            ✕ Clear all
          </button>

          <button class="btn ghost" type="button" id="clearSortsBtn" title="Clear sorts only">
            ↕ Clear sorts
          </button>

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
              <span class="x" role="button" tabindex="0" title="Remove">×</span>
            </span>
          {% endif %}
          {% if sorts.get(col) %}
            <span class="chip sort" data-kind="sort" data-col="{{ col }}">
              Sort <b>{{ col }}</b>: {{ '▲' if sorts.get(col)=='asc' else '▼' }}
              <span class="x" role="button" tabindex="0" title="Remove">×</span>
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

                  <div class="th-bottom" data-action="sort" title="Click to sort">
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
                  <td class="{% if col in ['symbol'] %}mono{% endif %}">
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

  <script src="{{ url_for('static', filename='historical.js') }}"></script>
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

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    if sorts:
        order_parts = [f"{col} {direction}" for col, direction in sorts.items()]
        order_sql = "ORDER BY " + ", ".join(order_parts)
    else:
        order_sql = "ORDER BY quotedate DESC, symbol ASC, expiredate ASC, strike ASC"

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
        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result]

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
        filters=filters,
        sorts=sorts,
        limit=limit,
        fmt=fmt,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001)
