# ============================
# FILE 1: frontendStorage
# ============================

from flask import render_template_string, url_for
from sqlalchemy import text

HEADER_HTML = r"""
<div class="header">
  <div class="title">
    <h1>Storage Usage Over Time</h1>
    <div class="topnav">
      <a class="tab" href="/">Option Info</a>
      <a class="tab active" href="{{ url_for('storage_dashboard') }}">Storage Graph</a>
    </div>
    <div class="sub">
      Tracks daily disk usage snapshots (root + volume) from
      <code>disk_usage_daily</code>.
    </div>
  </div>

  <div class="pill">
    Latest snapshot: <code>{{ latest_date or "—" }}</code>
  </div>
</div>
"""

STORAGE_PAGE = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Storage Usage Over Time</title>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <link rel="stylesheet" href="{{ url_for('static', filename='storage.css') }}">
</head>
<body>
  <div class="container">
    """ + HEADER_HTML + r"""

    <div class="card">
      <div class="row">
        <div class="pill">X-axis: <code>Date</code></div>
        <div class="pill">Y-axis: <code>Percent Used</code></div>
      </div>

      {% if not points %}
        <div class="empty">
          No rows found in <code>disk_usage_daily</code> yet.
          Once your snapshot runs, refresh this page.
        </div>
      {% else %}
        <div class="chart-wrap">
          <canvas id="usageChart" aria-label="Storage usage chart"></canvas>
        </div>

        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Root Used %</th>
              <th>Root Used / Total (GB)</th>
              <th>Volume Used %</th>
              <th>Volume Used / Total (GB)</th>
            </tr>
          </thead>
          <tbody>
            {% for r in points %}
              <tr>
                <td>{{ r["date"] }}</td>
                <td>{{ "%.2f"|format(r["root_pct"]) }}%</td>
                <td>{{ "%.2f"|format(r["root_used_gb"]) }} / {{ "%.2f"|format(r["root_total_gb"]) }}</td>
                <td>{{ "%.2f"|format(r["vol_pct"]) }}%</td>
                <td>{{ "%.2f"|format(r["vol_used_gb"]) }} / {{ "%.2f"|format(r["vol_total_gb"]) }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% endif %}
    </div>
  </div>

  <script>
    window.STORAGE_LABELS = {{ labels | tojson }};
    window.STORAGE_ROOT_PCT = {{ root_pct | tojson }};
    window.STORAGE_VOL_PCT  = {{ vol_pct  | tojson }};
  </script>
  <script src="{{ url_for('static', filename='storage.js') }}"></script>
</body>
</html>
"""

def _bytes_to_gb(x: int) -> float:
    return float(x) / (1024.0 ** 3)

def register_storage_routes(app, engine):
    @app.route("/storage", methods=["GET"])
    def storage_dashboard():
        sql = """
        SELECT
          captured_at::date AS d,
          root_total_bytes,
          root_used_bytes,
          vol_total_bytes,
          vol_used_bytes
        FROM disk_usage_daily
        ORDER BY captured_at::date ASC
        """
        with engine.connect() as conn:
            rows = [dict(r._mapping) for r in conn.execute(text(sql))]

        points = []
        labels = []
        root_pct = []
        vol_pct = []

        for r in rows:
            d = r["d"].isoformat()
            rt = int(r["root_total_bytes"])
            ru = int(r["root_used_bytes"])
            vt = int(r["vol_total_bytes"])
            vu = int(r["vol_used_bytes"])

            rp = (ru / rt * 100.0) if rt else 0.0
            vp = (vu / vt * 100.0) if vt else 0.0

            labels.append(d)
            root_pct.append(round(rp, 4))
            vol_pct.append(round(vp, 4))

            points.append({
                "date": d,
                "root_pct": rp,
                "root_used_gb": _bytes_to_gb(ru),
                "root_total_gb": _bytes_to_gb(rt),
                "vol_pct": vp,
                "vol_used_gb": _bytes_to_gb(vu),
                "vol_total_gb": _bytes_to_gb(vt),
            })

        latest_date = labels[-1] if labels else None

        return render_template_string(
            STORAGE_PAGE,
            points=points,
            labels=labels,
            root_pct=root_pct,
            vol_pct=vol_pct,
            latest_date=latest_date,
        )
