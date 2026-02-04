# app.py
@app.route("/atmoption")
def atmoption():
    return render_template(
        "atmoption.html",
        active_page="atmoption",
        disk=get_disk_usage()
    )
