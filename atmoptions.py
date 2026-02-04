# atmoptions.py
from flask import request, render_template_string

def register_atmoptions_route(app):
    # shared layout + helpers (HTML lives in frontendOptions.py)
    from frontendOptions import TABLE_PAGE, fmt, get_latest_disk_status

    ATM_COLUMNS = [
        "quotedate",
        "underlyinglast",
        "expiredate",
        "dte",
        "direction",
        "strike",
        "open",
        "close",
        "diff",
        "perc",
    ]

    @app.route("/atmoption", methods=["GET"])
    def atmoption():
        filters = {}
        sorts = {}

        # same filter/sort URL behavior as other pages
        for col in ATM_COLUMNS:
            fval = request.args.get(f"f_{col}")
            sval = request.args.get(f"s_{col}")

            if fval:
                filters[col] = fval
            if sval in ("asc", "desc"):
                sorts[col] = sval

        limit = int(request.args.get("limit", 100))

        # UI-only for now — no backend data yet
        rows = []

        return render_template_string(
            TABLE_PAGE,
            active_page="atmoption",
            rows=rows,
            columns=ATM_COLUMNS,
            filters=filters,
            sorts=sorts,
            limit=limit,
            fmt=fmt,
            disk=get_latest_disk_status(),
        )
