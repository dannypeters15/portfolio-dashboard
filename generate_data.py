"""
Portfolio Risk Dashboard — Data Generator
Fetches full price history for all stocks, calculates log-linear regression
risk scores, and writes data.json for the dashboard to consume.
Runs via GitHub Actions daily after US market close.
"""

import json, math, os, subprocess
from datetime import date, datetime, timedelta
import urllib.request, urllib.error

# ── Watchlist ──────────────────────────────────────────────────────────────────
STOCK_LIST = {
    "MSFT":   {"name": "Microsoft",              "inception": "1986-03-13", "allocation": 5},
    "AMZN":   {"name": "Amazon",                 "inception": "1997-05-15", "allocation": 5},
    "NVDA":   {"name": "Nvidia",                 "inception": "1999-01-22", "allocation": 5},
    "META":   {"name": "Meta Platforms",         "inception": "2012-05-18", "allocation": 5},
    "ORCL":   {"name": "Oracle",                 "inception": "1986-03-12", "allocation": 5},
    "AVGO":   {"name": "Broadcom",               "inception": "2009-08-06", "allocation": 5},
    "NFLX":   {"name": "Netflix",                "inception": "2002-05-23", "allocation": 5},
    "TSLA":   {"name": "Tesla",                  "inception": "2010-06-29", "allocation": 5},
    "AMD":    {"name": "AMD",                    "inception": "1972-09-27", "allocation": 5},
    "PLTR":   {"name": "Palantir",               "inception": "2020-09-30", "allocation": 5},
    "SPOT":   {"name": "Spotify",                "inception": "2018-04-03", "allocation": 5},
    "MSTR":   {"name": "MicroStrategy",          "inception": "1998-06-11", "allocation": 5},
    "TTWO":   {"name": "Take-Two Interactive",   "inception": "1993-09-24", "allocation": 5},
    "TGT":    {"name": "Target",                 "inception": "1967-01-01", "allocation": 5},
    "SONY":   {"name": "Sony Group",             "inception": "1970-09-01", "allocation": 5},
    "IREN":   {"name": "Iris Energy",            "inception": "2021-11-18", "allocation": 5},
    "MP":     {"name": "MP Materials",           "inception": "2020-11-17", "allocation": 5},
    "USAR":   {"name": "USA Rare Earth",         "inception": "2024-01-01", "allocation": 5},
    "URA":    {"name": "Global X Uranium ETF",   "inception": "2010-11-04", "allocation": 5},
    "IFC.TO": {"name": "Intact Financial",       "inception": "2004-02-12", "allocation": 5},
    "RR.L":   {"name": "Rolls-Royce",            "inception": "1987-01-01", "allocation": 5},
}


# ── Data Fetching ──────────────────────────────────────────────────────────────
def fetch_yahoo(ticker):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=max"
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; risk-dashboard/1.0)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            data = json.loads(r.read())
        result = data["chart"]["result"][0]
        timestamps = result["timestamp"]
        closes    = result["indicators"]["quote"][0]["close"]
        points = [
            (date.fromtimestamp(t), c)
            for t, c in zip(timestamps, closes)
            if c is not None and c > 0
        ]
        return sorted(points, key=lambda x: x[0])
    except Exception as e:
        print(f"  ⚠ Fetch failed for {ticker}: {e}")
        return []


# ── Maths ──────────────────────────────────────────────────────────────────────
def linear_regression(xs, ys):
    n = len(xs)
    if n < 10:
        return 0, 0
    sx  = sum(xs);  sy  = sum(ys)
    sxy = sum(x*y for x, y in zip(xs, ys))
    sx2 = sum(x*x for x in xs)
    denom = n*sx2 - sx*sx
    if denom == 0:
        return 0, 0
    slope     = (n*sxy - sx*sy) / denom
    intercept = (sy - slope*sx) / n
    return slope, intercept


def calculate_risk(history, inception_str):
    inception = date.fromisoformat(inception_str)
    data = [(d, p) for d, p in history if d >= inception and p > 0]
    if len(data) < 30:
        return None

    def days(d): return (d - inception).days

    xs = [days(d) for d, _ in data]
    ys = [math.log10(p) for _, p in data]
    slope, intercept = linear_regression(xs, ys)

    predicted = [slope*x + intercept for x in xs]
    residuals = [y - p for y, p in zip(ys, predicted)]
    min_res   = min(residuals)
    max_res   = max(residuals)
    span      = max_res - min_res
    if span == 0:
        return None

    risks = [(r - min_res) / span for r in residuals]

    last_d, last_p = data[-1]
    last_x    = days(last_d)
    last_risk = risks[-1]
    fair_val  = 10 ** (slope * last_x + intercept)
    deviation = (last_p - fair_val) / fair_val

    # 1-year chart data
    cutoff_1y = date.today() - timedelta(days=365)
    chart_dates, chart_prices, chart_risks = [], [], []
    for i, (d, p) in enumerate(data):
        if d >= cutoff_1y:
            chart_dates.append(d.isoformat())
            chart_prices.append(round(p, 2))
            chart_risks.append(round(risks[i], 4))

    # 1-year return
    one_year_ago_idx = 0
    for i, (d, _) in enumerate(data):
        if d >= cutoff_1y:
            one_year_ago_idx = i
            break
    ret_1y = (last_p - data[one_year_ago_idx][1]) / data[one_year_ago_idx][1] if one_year_ago_idx else None

    return {
        "price":      round(last_p, 2),
        "risk":       round(last_risk, 4),
        "fair_value": round(fair_val, 2),
        "deviation":  round(deviation, 4),
        "zone":       risk_zone(last_risk),
        "ret_1y":     round(ret_1y, 4) if ret_1y is not None else None,
        "chart": {
            "dates":  chart_dates,
            "prices": chart_prices,
            "risks":  chart_risks,
        },
    }


def risk_zone(r):
    if r < 0.35: return "ACCUMULATE"
    if r < 0.65: return "NEUTRAL"
    if r < 0.85: return "CAUTION"
    return "EXTREME"


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"Portfolio Risk Dashboard — Data Update")
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stocks":    {},
        "summary":   {"accumulate": 0, "neutral": 0, "caution": 0, "extreme": 0, "unknown": 0},
    }

    for ticker, cfg in STOCK_LIST.items():
        print(f"Fetching {ticker} ({cfg['name']})...")
        history = fetch_yahoo(ticker)

        if not history:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "error": "No data available",
            }
            output["summary"]["unknown"] += 1
            continue

        result = calculate_risk(history, cfg["inception"])
        if not result:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "error": "Insufficient data",
            }
            output["summary"]["unknown"] += 1
            continue

        output["stocks"][ticker] = {
            "name": cfg["name"], "ticker": ticker,
            "allocation": cfg["allocation"], **result,
        }
        zone_key = result["zone"].lower()
        output["summary"][zone_key] = output["summary"].get(zone_key, 0) + 1

        print(f"  ${result['price']}  |  Risk {result['risk']:.3f}  |  {result['zone']}")

    # Write data.json
    with open("data.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))
    size_kb = len(json.dumps(output)) // 1024
    print(f"\n✅ Wrote data.json ({size_kb} KB)")

    # Ensure ideas.json exists (never overwrite)
    if not os.path.exists("ideas.json"):
        with open("ideas.json", "w") as f:
            json.dump({"ideas": []}, f)
        print("✅ Created ideas.json")

    # Git commit & push
    try:
        subprocess.run(["git", "config", "user.name",  "Dashboard Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "bot@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", "data.json", "ideas.json"], check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode != 0:
            msg = f"data: update {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            subprocess.run(["git", "commit", "-m", msg], check=True)
            subprocess.run(["git", "push"],               check=True)
            print("✅ Committed and pushed")
        else:
            print("ℹ No changes to commit")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git error: {e}")
        raise


if __name__ == "__main__":
    main()
