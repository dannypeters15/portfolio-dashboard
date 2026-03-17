"""
Portfolio Risk Dashboard v2 — Comprehensive Data Generator
- Three risk calculation methods based on history length
- Fundamentals: P/E, market cap, 52-week high/low
- Historical P/E chart from earnings history
- Multi-year returns (1–5 year)
- RSI + Bollinger Band calculations
- Sector grouping + correlation data
- News headlines
"""

import json, math, os, subprocess, time
from datetime import date, datetime, timedelta
import urllib.request, urllib.error

# ── Sector mapping ─────────────────────────────────────────────────────────────
SECTORS = {
    "MSFT": "Technology",       "AMZN": "Technology",      "NVDA": "Semiconductors",
    "META": "Technology",       "AAPL": "Technology",      "GOOG": "Technology",
    "IBM":  "Technology",       "ADBE": "Technology",      "ORCL": "Technology",
    "PLTR": "Technology",       "SHOP": "Technology",      "PATH": "Technology",
    "CSCO": "Technology",       "AVGO": "Semiconductors",  "AMD":  "Semiconductors",
    "INTC": "Semiconductors",   "NFLX": "Consumer",        "SPOT": "Consumer",
    "TSLA": "EV / Auto",        "COST": "Consumer",        "WMT":  "Consumer",
    "TGT":  "Consumer",         "BABA": "Consumer",        "SONY": "Consumer",
    "TTWO": "Gaming",           "JPM":  "Financials",      "BRK-B":"Financials",
    "BLK":  "Financials",       "HOOD": "Financials",      "IFC.TO":"Financials",
    "TMUS": "Telecom",          "CVX":  "Energy",          "DVN":  "Energy",
    "URA":  "Uranium / Nuclear","UEC":  "Uranium / Nuclear","CCO.TO":"Uranium / Nuclear",
    "UUUU": "Uranium / Nuclear","MP":   "Rare Earth",      "USAR": "Rare Earth",
    "ALB":  "Materials",        "LAC":  "Materials",       "AG":   "Materials",
    "MSTR": "Crypto / Bitcoin", "IREN": "Crypto / Bitcoin","BMNR": "Crypto / Bitcoin",
    "APLD": "Crypto / Bitcoin", "AVAV": "Defence",         "RR.L": "Defence",
    "XPEV": "EV / Auto",        "RIVN": "EV / Auto",       "ONDS": "Small Cap",
    "SRFM": "Small Cap",        "CCO":  "Media",
}

# ── Watchlist ──────────────────────────────────────────────────────────────────
STOCK_LIST = {
    "MSFT":   {"name": "Microsoft",              "inception": "1986-03-13", "allocation": 3},
    "AMZN":   {"name": "Amazon",                 "inception": "1997-05-15", "allocation": 3},
    "NVDA":   {"name": "Nvidia",                 "inception": "1999-01-22", "allocation": 3},
    "META":   {"name": "Meta Platforms",         "inception": "2012-05-18", "allocation": 3},
    "AAPL":   {"name": "Apple",                  "inception": "1980-12-12", "allocation": 3},
    "GOOG":   {"name": "Alphabet",               "inception": "2004-08-19", "allocation": 3},
    "IBM":    {"name": "IBM",                    "inception": "1962-01-02", "allocation": 2},
    "ADBE":   {"name": "Adobe",                  "inception": "1986-08-13", "allocation": 2},
    "ORCL":   {"name": "Oracle",                 "inception": "1986-03-12", "allocation": 2},
    "PLTR":   {"name": "Palantir",               "inception": "2020-09-30", "allocation": 2},
    "SHOP":   {"name": "Shopify",                "inception": "2015-05-21", "allocation": 2},
    "PATH":   {"name": "UiPath",                 "inception": "2021-04-21", "allocation": 2},
    "CSCO":   {"name": "Cisco",                  "inception": "1990-02-16", "allocation": 2},
    "AVGO":   {"name": "Broadcom",               "inception": "2009-08-06", "allocation": 2},
    "AMD":    {"name": "AMD",                    "inception": "1972-09-27", "allocation": 2},
    "INTC":   {"name": "Intel",                  "inception": "1971-10-13", "allocation": 2},
    "NFLX":   {"name": "Netflix",                "inception": "2002-05-23", "allocation": 2},
    "SPOT":   {"name": "Spotify",                "inception": "2018-04-03", "allocation": 2},
    "TSLA":   {"name": "Tesla",                  "inception": "2010-06-29", "allocation": 2},
    "COST":   {"name": "Costco",                 "inception": "1985-12-05", "allocation": 2},
    "WMT":    {"name": "Walmart",                "inception": "1972-08-25", "allocation": 2},
    "TGT":    {"name": "Target",                 "inception": "1967-01-03", "allocation": 2},
    "BABA":   {"name": "Alibaba",                "inception": "2014-09-19", "allocation": 2},
    "SONY":   {"name": "Sony Group",             "inception": "1970-09-01", "allocation": 2},
    "TTWO":   {"name": "Take-Two Interactive",   "inception": "1993-09-24", "allocation": 2},
    "JPM":    {"name": "JPMorgan Chase",         "inception": "1969-01-02", "allocation": 2},
    "BRK-B":  {"name": "Berkshire Hathaway B",   "inception": "1996-05-09", "allocation": 2},
    "BLK":    {"name": "BlackRock",              "inception": "1999-10-01", "allocation": 2},
    "HOOD":   {"name": "Robinhood",              "inception": "2021-07-29", "allocation": 2},
    "IFC.TO": {"name": "Intact Financial",       "inception": "2004-02-12", "allocation": 2},
    "TMUS":   {"name": "T-Mobile US",            "inception": "2013-05-01", "allocation": 2},
    "CVX":    {"name": "Chevron",                "inception": "1970-01-02", "allocation": 2},
    "DVN":    {"name": "Devon Energy",           "inception": "1988-01-04", "allocation": 2},
    "URA":    {"name": "Global X Uranium ETF",   "inception": "2010-11-04", "allocation": 2},
    "UEC":    {"name": "Uranium Energy Corp",    "inception": "2007-01-03", "allocation": 2},
    "CCO.TO": {"name": "Cameco",                 "inception": "1991-01-02", "allocation": 2},
    "UUUU":   {"name": "Energy Fuels",           "inception": "2012-01-03", "allocation": 2},
    "MP":     {"name": "MP Materials",           "inception": "2020-11-17", "allocation": 2},
    "USAR":   {"name": "USA Rare Earth",         "inception": "2024-01-02", "allocation": 2},
    "ALB":    {"name": "Albemarle",              "inception": "1994-02-28", "allocation": 2},
    "LAC":    {"name": "Lithium Americas",       "inception": "2020-09-01", "allocation": 2},
    "AG":     {"name": "First Majestic Silver",  "inception": "2011-05-10", "allocation": 2},
    "MSTR":   {"name": "MicroStrategy",          "inception": "1998-06-11", "allocation": 2},
    "IREN":   {"name": "Iris Energy",            "inception": "2021-11-18", "allocation": 2},
    "BMNR":   {"name": "Bitmine Immersion",      "inception": "2022-06-01", "allocation": 2},
    "APLD":   {"name": "Applied Digital",        "inception": "2022-04-01", "allocation": 2},
    "AVAV":   {"name": "AeroVironment",          "inception": "2007-01-26", "allocation": 2},
    "RR.L":   {"name": "Rolls-Royce",            "inception": "1987-01-01", "allocation": 2},
    "XPEV":   {"name": "XPeng",                  "inception": "2020-08-27", "allocation": 2},
    "RIVN":   {"name": "Rivian",                 "inception": "2021-11-10", "allocation": 2},
    "ONDS":   {"name": "Ondas Holdings",         "inception": "2018-01-02", "allocation": 1},
    "SRFM":   {"name": "Surf Air Mobility",      "inception": "2023-07-27", "allocation": 1},
    "CCO":    {"name": "Clear Channel Outdoor",  "inception": "2005-11-11", "allocation": 1},
}

# Risk method thresholds (days of history required)
STRUCTURAL_MIN_DAYS = 1825   # 5+ years → log-linear regression + sigma bands
TECHNICAL_MIN_DAYS  = 730    # 2–5 years → RSI + Bollinger composite
# < 730 days → Early Stage (price percentile)

# Number of standard deviations for risk band edges
SIGMA_LOWER = 2.0   # regression - 2σ = risk 0.0
SIGMA_UPPER = 2.0   # regression + 2σ = risk 1.0


# ── HTTP helper ────────────────────────────────────────────────────────────────
def yahoo_get(url, timeout=20):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; portfolio-dashboard/2.0)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── Data fetchers ──────────────────────────────────────────────────────────────
def fetch_history(ticker):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=max"
    try:
        data = yahoo_get(url)
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        pts = [(date.fromtimestamp(t), c) for t, c in zip(ts, closes) if c and c > 0]
        return sorted(pts)
    except Exception as e:
        print(f"  ⚠ History failed for {ticker}: {e}")
        return []


def fetch_fundamentals(ticker):
    """
    Robust fundamentals fetch using Yahoo Finance v8 chart API for P/E
    and quoteSummary for stats. The chart API is far more reliable for
    earnings data as it's the same endpoint TradingView uses.
    """
    def val(d, k):
        v = d.get(k)
        return v.get("raw") if isinstance(v, dict) else v

    result = {"earnings_history": []}

    # ── Step 1: Summary stats (market cap, 52wk, PE ratio) ──────────────
    for host in ["query2", "query1"]:
        url = (f"https://{host}.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
               f"?modules=defaultKeyStatistics%2CsummaryDetail%2CfinancialData%2Cprice")
        try:
            data = yahoo_get(url)
            res  = data["quoteSummary"]["result"][0]
            ks   = res.get("defaultKeyStatistics", {})
            sd   = res.get("summaryDetail", {})
            fd   = res.get("financialData", {})
            pr   = res.get("price", {})
            result.update({
                "trailing_pe": val(ks,"trailingPE") or val(pr,"trailingPE"),
                "forward_pe":  val(ks,"forwardPE"),
                "market_cap":  val(sd,"marketCap") or val(pr,"marketCap"),
                "week52_high": val(sd,"fiftyTwoWeekHigh"),
                "week52_low":  val(sd,"fiftyTwoWeekLow"),
                "ps_ratio":    val(sd,"priceToSalesTrailingTwelveMonths"),
                "eps_ttm":     val(fd,"trailingEps") or val(ks,"trailingEps"),
            })
            print(f"  PE={result.get('trailing_pe','—')}  Cap={result.get('market_cap','—')}")
            break
        except Exception as e:
            print(f"  ⚠ Stats ({host}) failed: {e}")

    time.sleep(0.5)

    # ── Step 2: Earnings history via v8 chart (most reliable) ───────────
    # Yahoo's chart API includes quarterly EPS in the events section
    for period in ["max", "10y"]:
        url = (f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
               f"?interval=3mo&range={period}&includePrePost=false")
        try:
            data   = yahoo_get(url)
            result_chart = data["chart"]["result"][0]
            # Quarterly closes + timestamps give us price at each quarter
            ts     = result_chart.get("timestamp", [])
            closes = result_chart["indicators"]["quote"][0].get("close", [])

            # Check for earnings in meta
            events = result_chart.get("events", {})
            earnings_raw = events.get("earnings", {})

            if earnings_raw:
                eh = []
                for k_ts, ev in earnings_raw.items():
                    eps = ev.get("epsActual") or ev.get("eps")
                    if eps is not None and eps != 0:
                        eh.append({
                            "quarter":   {"raw": int(k_ts)},
                            "epsActual": {"raw": float(eps)},
                        })
                if eh:
                    result["earnings_history"] = sorted(eh, key=lambda x: x["quarter"]["raw"])
                    print(f"  ✅ Chart API earnings: {len(eh)} quarters")
                    break
        except Exception as e:
            print(f"  ⚠ Chart earnings ({period}) failed: {e}")

    # ── Step 3: earningsHistory module fallback ──────────────────────────
    if not result["earnings_history"]:
        time.sleep(0.4)
        for host in ["query2", "query1"]:
            url = (f"https://{host}.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
                   f"?modules=earningsHistory")
            try:
                data = yahoo_get(url)
                res  = data["quoteSummary"]["result"][0]
                eh   = res.get("earningsHistory", {}).get("history", [])
                if eh:
                    result["earnings_history"] = eh
                    print(f"  ✅ earningsHistory module: {len(eh)} quarters")
                    break
            except Exception as e:
                print(f"  ⚠ earningsHistory ({host}) failed: {e}")

    # ── Step 4: incomeStatementHistoryQuarterly deep fallback ────────────
    if not result["earnings_history"]:
        time.sleep(0.4)
        url = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
               f"?modules=incomeStatementHistoryQuarterly")
        try:
            data  = yahoo_get(url)
            res   = data["quoteSummary"]["result"][0]
            stmts = res.get("incomeStatementHistoryQuarterly",{}).get("incomeStatementHistory",[])
            eh = []
            for s in stmts:
                q_date  = s.get("endDate", {})
                eps_raw = s.get("dilutedEPS", {})
                ni      = s.get("netIncome", {})
                shares  = s.get("dilutedAverageShares", {})
                if isinstance(q_date, dict):
                    ts_val = q_date.get("raw", 0)
                    # Use diluted EPS if available, else compute from net income / shares
                    if isinstance(eps_raw, dict) and eps_raw.get("raw") is not None:
                        eps = eps_raw["raw"]
                    elif isinstance(ni, dict) and isinstance(shares, dict):
                        ni_v = ni.get("raw", 0)
                        sh_v = shares.get("raw", 1)
                        eps  = ni_v / sh_v if sh_v else None
                    else:
                        eps = None
                    if eps is not None and ts_val:
                        eh.append({"quarter":{"raw":ts_val},"epsActual":{"raw":float(eps)}})
            if eh:
                result["earnings_history"] = sorted(eh, key=lambda x: x["quarter"]["raw"])
                print(f"  ✅ Income stmt fallback: {len(eh)} quarters")
        except Exception as e:
            print(f"  ⚠ Income stmt fallback failed: {e}")

    if not result["earnings_history"]:
        print(f"  ℹ No earnings history found for {ticker} — P/E chart will be unavailable")

    return result


def fetch_news(ticker):
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=3&enableFuzzyQuery=false"
    try:
        data = yahoo_get(url)
        items = data.get("news", [])
        return [
            {
                "title":     i.get("title", ""),
                "url":       i.get("link", ""),
                "publisher": i.get("publisher", ""),
                "date":      i.get("providerPublishTime", 0),
            }
            for i in items[:3]
        ]
    except Exception as e:
        print(f"  ⚠ News failed for {ticker}: {e}")
        return []


# ── Maths ──────────────────────────────────────────────────────────────────────
def linear_regression(xs, ys):
    n = len(xs)
    if n < 10: return 0, 0
    sx = sum(xs); sy = sum(ys)
    sxy = sum(x*y for x, y in zip(xs, ys))
    sx2 = sum(x*x for x in xs)
    d = n*sx2 - sx*sx
    if d == 0: return 0, 0
    slope = (n*sxy - sx*sy) / d
    return slope, (sy - slope*sx) / n


def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        chg = prices[i] - prices[i-1]
        gains.append(max(0, chg))
        losses.append(max(0, -chg))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))


def calculate_bb_position(prices, period=20):
    if len(prices) < period: return 0.5
    recent = prices[-period:]
    ma  = sum(recent) / period
    std = (sum((p-ma)**2 for p in recent) / period) ** 0.5
    if std == 0: return 0.5
    pos = (prices[-1] - (ma - 2*std)) / (4*std)
    return max(0.0, min(1.0, pos))


def risk_zone(r):
    if r < 0.35: return "ACCUMULATE"
    if r < 0.65: return "NEUTRAL"
    if r < 0.85: return "CAUTION"
    return "EXTREME"


# ── Risk calculation ───────────────────────────────────────────────────────────
def calculate_risk(history, inception_str):
    inception = date.fromisoformat(inception_str)
    data = [(d, p) for d, p in history if d >= inception and p > 0]
    if len(data) < 10: return None

    days_history = (data[-1][0] - data[0][0]).days
    prices       = [p for _, p in data]

    def days_since(d): return (d - inception).days

    full_risks = []

    if days_history >= STRUCTURAL_MIN_DAYS:
        method = "Structural"
        xs = [days_since(d) for d, _ in data]
        ys = [math.log10(p) for _, p in data]
        slope, intercept = linear_regression(xs, ys)
        predicted = [slope*x + intercept for x in xs]
        residuals = [y - p for y, p in zip(ys, predicted)]

        # σ-band normalisation (Cowen-style dynamic bands)
        # Uses rolling 252-day std dev for responsiveness, falls back to full history
        std_full = (sum(r**2 for r in residuals) / len(residuals)) ** 0.5
        # Use full-history sigma for band anchoring (stable, consistent)
        lower_band = -SIGMA_LOWER * std_full   # risk 0.0 anchor
        upper_band =  SIGMA_UPPER * std_full   # risk 1.0 anchor
        band_range = upper_band - lower_band

        if band_range == 0: return None

        full_risks     = [max(0.0, min(1.0, (r - lower_band) / band_range)) for r in residuals]
        current_risk   = full_risks[-1]
        last_x         = xs[-1]
        fair_value     = 10 ** (slope * last_x + intercept)
        deviation      = (prices[-1] - fair_value) / fair_value

        # Also compute what price corresponds to each risk level (like Cowen's tables)
        risk_price_map = {}
        for rv in [i/20 for i in range(21)]:
            # residual = lower_band + rv * band_range
            target_log = (slope * last_x + intercept) + (lower_band + rv * band_range)
            risk_price_map[round(rv, 3)] = round(10 ** target_log, 2)

    elif days_history >= TECHNICAL_MIN_DAYS:
        method = "Technical"
        rsi_val = calculate_rsi(prices)
        bb_val  = calculate_bb_position(prices)
        current_risk = 0.5 * (rsi_val / 100) + 0.5 * bb_val
        fair_value = deviation = None
        # rolling risk for chart
        for i in range(len(prices)):
            sl = prices[max(0, i-60):i+1]
            r  = calculate_rsi(sl)
            b  = calculate_bb_position(sl)
            full_risks.append(round(0.5*(r/100) + 0.5*b, 4))

    else:
        method = "Early Stage"
        mn_p, mx_p = min(prices), max(prices)
        current_risk = (prices[-1] - mn_p) / (mx_p - mn_p) if mx_p != mn_p else 0.5
        fair_value = deviation = None
        # rolling percentile
        for i in range(len(prices)):
            sl = prices[:i+1]
            mn2, mx2 = min(sl), max(sl)
            full_risks.append((sl[-1] - mn2) / (mx2 - mn2) if mx2 != mn2 else 0.5)

    # ── RSI and BB always computed for display ──
    rsi_display = calculate_rsi(prices)
    bb_display  = calculate_bb_position(prices)

    # ── Multi-year returns ──
    returns = {}
    last_d, last_p = data[-1]
    for yrs in [1, 2, 3, 4, 5]:
        target = date(last_d.year - yrs, last_d.month, last_d.day)
        candidates = [(abs((d - target).days), p) for d, p in data if abs((d-target).days) < 30]
        if candidates:
            _, past_p = min(candidates)
            returns[f"ret_{yrs}y"] = round((last_p - past_p) / past_p, 4)
        else:
            returns[f"ret_{yrs}y"] = None

    # ── Historical risk scores (annual snapshots) ──
    hist_risk_scores = []
    for yr_offset in range(1, 6):
        target = date(last_d.year - yr_offset, last_d.month, last_d.day)
        candidates = [(abs((d - target).days), i) for i, (d, _) in enumerate(data) if abs((d-target).days) < 30]
        if candidates and full_risks:
            _, idx = min(candidates)
            if idx < len(full_risks):
                hist_risk_scores.append({
                    "year": target.year,
                    "risk": round(full_risks[idx], 4)
                })

    # ── Chart data (2 years weekly ~104 points) ──
    cutoff = date.today() - timedelta(days=730)
    chart_data_raw = [(d, p, full_risks[i]) for i, (d, p) in enumerate(data) if d >= cutoff]
    if len(chart_data_raw) < 30:
        chart_data_raw = [(d, p, full_risks[i]) for i, (d, p) in enumerate(data)][-104:]

    # Downsample
    if len(chart_data_raw) > 104:
        step = max(1, len(chart_data_raw) // 104)
        chart_data_raw = chart_data_raw[::step]

    chart = {
        "dates":  [d.isoformat() for d, _, _ in chart_data_raw],
        "prices": [round(p, 2) for _, p, _ in chart_data_raw],
        "risks":  [round(r, 4) for _, _, r in chart_data_raw],
    }

    return {
        "price":          round(last_p, 2),
        "risk":           round(current_risk, 4),
        "risk_method":    method,
        "zone":           risk_zone(current_risk),
        "fair_value":     round(fair_value, 2) if fair_value else None,
        "deviation":      round(deviation, 4) if deviation is not None else None,
        "rsi":            round(rsi_display, 1),
        "bb_position":    round(bb_display, 3),
        "hist_risks":     hist_risk_scores,
        "risk_price_map": locals().get('risk_price_map'),
        "chart":          chart,
        **returns,
    }


def calculate_pe_history(history, fundamentals):
    """Trailing P/E at each earnings date (sum of last 4 quarters EPS)."""
    earnings_history = fundamentals.get("earnings_history", [])
    eps_ttm          = fundamentals.get("eps_ttm")

    def get_raw(d, k):
        v = d.get(k)
        return v.get("raw") if isinstance(v, dict) else v

    pts = []

    # Primary: build rolling TTM P/E from earnings history
    if earnings_history and len(earnings_history) >= 4:
        try:
            eh = sorted(earnings_history, key=lambda x: get_raw(x, "quarter") or 0)
            for i in range(3, len(eh)):
                q_ts = get_raw(eh[i], "quarter")
                if not q_ts: continue
                q_date = date.fromtimestamp(q_ts)
                eps_vals = []
                for j in range(4):
                    e = get_raw(eh[i-j], "epsActual")
                    if e is not None: eps_vals.append(e)
                if len(eps_vals) < 4: continue
                ttm = sum(eps_vals)
                if ttm <= 0: continue
                nearby = [(abs((d2 - q_date).days), p) for d2, p in history if abs((d2-q_date).days) < 25]
                if not nearby: continue
                _, price = min(nearby)
                pe = price / ttm
                if 0 < pe < 1000:
                    pts.append({"date": q_date.isoformat(), "pe": round(pe, 1)})
        except Exception as e:
            print(f"  ⚠ PE history calculation failed: {e}")

    # Fallback: if we have eps_ttm, compute current P/E as a single point
    if not pts and eps_ttm and eps_ttm > 0 and history:
        last_price = history[-1][1]
        pe = last_price / eps_ttm
        if 0 < pe < 1000:
            pts.append({"date": history[-1][0].isoformat(), "pe": round(pe, 1)})
            print(f"  ℹ Using EPS TTM fallback for P/E: {pe:.1f}x")

    return pts[-20:]


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"Portfolio Dashboard v2 — Data Update")
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stocks":    {},
        "summary":   {"accumulate": 0, "neutral": 0, "caution": 0, "extreme": 0, "unknown": 0},
        "sectors":   {},
    }

    for ticker, cfg in STOCK_LIST.items():
        print(f"\n[{ticker}] {cfg['name']}")
        sector = SECTORS.get(ticker, "Other")

        history      = fetch_history(ticker);      time.sleep(0.3)
        fundamentals = fetch_fundamentals(ticker); time.sleep(0.3)
        news         = fetch_news(ticker);         time.sleep(0.2)

        if not history:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "sector": sector,
                "error": "No price data",
            }
            output["summary"]["unknown"] += 1
            continue

        risk_data = calculate_risk(history, cfg["inception"])
        if not risk_data:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "sector": sector,
                "error": "Insufficient history for risk calculation",
            }
            output["summary"]["unknown"] += 1
            continue

        pe_hist = calculate_pe_history(history, fundamentals)

        def v(k): return fundamentals.get(k)

        output["stocks"][ticker] = {
            "name":        cfg["name"],
            "ticker":      ticker,
            "allocation":  cfg["allocation"],
            "sector":      sector,
            "trailing_pe": v("trailing_pe"),
            "forward_pe":  v("forward_pe"),
            "market_cap":  v("market_cap"),
            "week52_high": v("week52_high"),
            "week52_low":  v("week52_low"),
            "ps_ratio":    v("ps_ratio"),
            "pe_history":  pe_hist,
            "news":        news,
            "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            **risk_data,
        }

        zone = risk_data["zone"].lower()
        output["summary"][zone] = output["summary"].get(zone, 0) + 1

        # Sector aggregation
        if sector not in output["sectors"]:
            output["sectors"][sector] = {"tickers": [], "avg_risk": None}
        output["sectors"][sector]["tickers"].append(ticker)

        print(f"  ${risk_data['price']:>10}  |  Risk {risk_data['risk']:.3f}  "
              f"|  {risk_data['zone']:<12}  |  {risk_data['risk_method']}")

    # Sector avg risk
    for sector, sdata in output["sectors"].items():
        valid = [output["stocks"][t]["risk"]
                 for t in sdata["tickers"]
                 if "risk" in output["stocks"].get(t, {})]
        if valid:
            output["sectors"][sector]["avg_risk"] = round(sum(valid)/len(valid), 4)

    with open("data.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"\n✅ data.json written ({len(json.dumps(output))//1024} KB)")

    if not os.path.exists("ideas.json"):
        with open("ideas.json", "w") as f:
            json.dump({"ideas": []}, f)
        print("✅ ideas.json created")

    # Git push
    try:
        subprocess.run(["git", "config", "user.name",  "Dashboard Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "bot@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", "data.json", "ideas.json"], check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff.returncode != 0:
            subprocess.run(["git", "commit", "-m",
                            f"data: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Pushed to GitHub")
        else:
            print("ℹ No changes to push")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git error: {e}")
        raise


if __name__ == "__main__":
    main()
