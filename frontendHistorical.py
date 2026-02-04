from flask import request, render_template_string, Response, url_for
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
import os
import csv
import io
from decimal import Decimal
import subprocess
import json
import shutil
from pathlib import Path

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

def _lsblk_detect_volume_mount() -> str | None:
    if shutil.which("lsblk") is None:
        return None
    try:
        out = subprocess.check_output(
            ["lsblk", "-J", "-b", "-o", "NAME,TYPE,SIZE,MOUNTPOINT"],
            text=True,
        )
        data = json.loads(out)
    except Exception:
        return None

    candidates: list[tuple[int, str]] = []

    def walk(nodes):
        for n in nodes or []:
            mnt = n.get("mountpoint")
            typ = n.get("type")
            size = n.get("size")
            children = n.get("children") or []

            if mnt and typ in ("part", "lvm", "crypt", "disk"):
                try:
                    sz = int(size)
                except Exception:
                    sz = 0

                bad_exact = {"/", "/boot", "/boot/efi"}
                bad_prefixes = ("/run", "/dev", "/proc", "/sys")

                if mnt in bad_exact:
                    pass
                elif any(mnt.startswith(p) for p in bad_prefixes):
                    pass
                else:
                    candidates.append((sz, mnt))

            if children:
                walk(children)

    walk((data or {}).get("blockdevices") or [])
    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def _df_detect_volume_mount() -> str | None:
    if shutil.which("df") is None:
        return None
    try:
        out = subprocess.check_output(["df", "-B1"], text=True)
    except Exception:
        return None

    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    candidates: list[tuple[int, str]] = []
    for ln in lines[1:]:
        parts = ln.split()
        if len(parts) < 6:
            continue

        fs, total_b, used_b, avail_b, usepct, mnt = (
            parts[0],
            parts[1],
            parts[2],
            parts[3],
            parts[4],
            parts[5],
        )

        if fs.startswith(("tmpfs", "udev")):
            continue
        if mnt in ("/", "/boot", "/boot/efi"):
            continue
        if mnt.startswith(("/run", "/dev", "/proc", "/sys")):
            continue

        try:
            tb = int(total_b)
        except Exception:
            continue

        candidates.append((tb, mnt))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def detect_volume_mount() -> str:
    env_mnt = os.environ.get("VOLUME_MOUNT_PATH")
    if env_mnt:
        return env_mnt

    mnt = _lsblk_detect_volume_mount()
    if mnt:
        return mnt

    mnt = _df_detect_volume_mount()
    if mnt:
        return mnt

    return "/"

def get_latest_disk_status():
    root_used, root_total, root_pct = _df_usage("/")

    volume_mount = detect_volume_mount()
    vol_used, vol_total, vol_pct = _df_usage(volume_mount)

    if volume_mount == "/":
        vol_used = vol_total = vol_pct = None

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
HEADER_HTML = Path("static/header.html").read_text()

# -------------------------
# PAGE TEMPLATE (filters + sorts + chips + clear buttons)
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

EMPTY_TABLE_PAGE = """
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
    <div class="card">
      <div class="empty">
        No data yet. The table <code>option_history_eod</code> does not exist.
        Run <code>backendHistorical.py</code> once to create it and insert rows.
      </div>
    </div>
  </div>
</body>
</html>
"""

def _is_missing_table_error(e: Exception) -> bool:
    orig = getattr(e, "orig", None)
    if orig is not None and orig.__class__.__name__ == "UndefinedTable":
        return True
    msg = str(e).lower()
    return "does not exist" in msg and "relation" in msg

# -------------------------
# ROUTE REGISTRATION
# -------------------------
def register_historical_routes(app):
    # DO NOT register /storage here. It is already registered once in frontendOptions.py.

    @app.route("/historical", methods=["GET"])
    def historical():
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

        try:
            with engine_hist.connect() as conn:
                rows = [dict(r._mapping) for r in conn.execute(text(sql), params)]
        except ProgrammingError as e:
            if _is_missing_table_error(e):
                return render_template_string(
                    EMPTY_TABLE_PAGE,
                    active_page="historical",
                    limit=limit,
                    disk=get_latest_disk_status(),
                )
            raise

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
            active_page="historical",
            rows=rows,
            columns=COLUMNS,
            filters=filters,
            sorts=sorts,
            limit=limit,
            fmt=fmt,
            disk=get_latest_disk_status(),
        )

