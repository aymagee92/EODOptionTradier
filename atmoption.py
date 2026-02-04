from flask import Flask, render_template_string

app = Flask(__name__)

@app.route("/atmoption")
def atmoption():
    return render_template_string("""
<!doctype html>
<html>
<head>
  <title>ATM Option Strategy</title>
  <link rel="stylesheet" href="/static/options.css">
</head>

<body>

  <div class="header">
    <div class="title">
      <h1>Option Price Data</h1>
      <div class="topnav">
        <a class="tab" href="/">Option Info</a>
        <a class="tab" href="/historical">Historical Options</a>
        <a class="tab" href="/storage">Storage Graph</a>
        <a class="tab" href="/stockdata">Stock Data</a>
        <a class="tab active" href="/atmoption">ATM Option</a>
      </div>
    </div>
  </div>

  <div class="content">
    <h2>ATM Option Strategy</h2>

    <table>
      <thead>
        <tr>
          <th>quotedate</th>
          <th>underlyinglast</th>
          <th>expiredate</th>
          <th>dte</th>
          <th>direction</th>
          <th>strike</th>
          <th>open</th>
          <th>close</th>
          <th>diff</th>
          <th>perc</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

</body>
</html>
""")

if __name__ == "__main__":
    app.run(debug=True)
