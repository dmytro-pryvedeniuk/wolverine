#!/usr/bin/env python3
import argparse, json, os, datetime

def main():
    parser = argparse.ArgumentParser(description="Generate HTML dashboard from test results JSON")
    parser.add_argument("input", help="Path to results.json")
    parser.add_argument("--output", required=True, help="Output folder for dashboard")
    args = parser.parse_args()

    # Load results
    with open(args.input) as f:
        data = json.load(f)

    # Ensure output folder exists
    os.makedirs(args.output, exist_ok=True)

    # Build HTML
    html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>TestOps Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background: #f0f0f0; }
    tr:nth-child(even) { background: #fafafa; }
  </style>
</head>
<body>
  <h1>TestOps Dashboard</h1>
  <table>
    <tr><th>Run ID</th><th>Branch</th><th>Total</th><th>Failures</th><th>Timestamp</th></tr>
"""
    for r in data:
        ts = datetime.datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
        html += f"<tr><td>{r['run_id']}</td><td>{r['branch']}</td><td>{r['total']}</td><td>{r['failures']}</td><td>{ts}</td></tr>\n"

    html += """</table>
</body>
</html>"""

    # Write HTML file
    with open(os.path.join(args.output, "index.html"), "w") as f:
        f.write(html)

if __name__ == "__main__":
    main()