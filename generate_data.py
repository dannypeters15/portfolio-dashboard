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

# Alpha Vantage API key — set as GitHub Secret ALPHA_VANTAGE_KEY
AV_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")

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
    "MSTR":   {"name": "MicroStrategy",          "inception": "2020-08-11", "allocation": 2},  # Bitcoin strategy pivot
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
    "MGY":    {"name": "Magnolia Oil & Gas",      "inception": "2018-07-19", "allocation": 2},
}

# Risk method thresholds (days of history required)
STRUCTURAL_MIN_DAYS = 1825   # 5+ years → log-linear regression + sigma bands
TECHNICAL_MIN_DAYS  = 730    # 2–5 years → RSI + Bollinger composite
# < 730 days → Early Stage (price percentile)

# Number of standard deviations for risk band edges
SIGMA_LOWER = 2.0   # regression - 2σ = risk 0.0
SIGMA_UPPER = 2.0   # regression + 2σ = risk 1.0

# Force specific method regardless of history length
# Use for stocks where long history is misleading (structural decline, spin-offs etc)
METHOD_OVERRIDES = {
    "INTC":  "Technical",   # 50yr history distorted by 2000s decline
    "SONY":  "Technical",   # ADR listing history unreliable pre-2000
    "IBM":   "Technical",   # Structural decline since 2010 distorts regression
    "BABA":  "Technical",   # Chinese ADR, political risk distorts trend
    "CCO":   "Technical",   # Heavily debt-laden, regression unreliable
    "MSTR":  "Technical",   # Pre-2020 software co history irrelevant to Bitcoin proxy
}


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


# All stocks split into two batches — alternate each day to stay within AV free tier (25/day)
# Batch A = odd days, Batch B = even days. Each batch ≤ 27 stocks.
# AV OVERVIEW rotation — 54 stocks split into 3 daily batches of 18
# Day mod 3 == 0 → batch 0 (stocks 0-17)
# Day mod 3 == 1 → batch 1 (stocks 18-35)  
# Day mod 3 == 2 → batch 2 (stocks 36-53)
_ALL_TICKERS_ORDERED = [
    "MSFT","AMZN","NVDA","META","AAPL","GOOG","IBM","ADBE",
    "ORCL","PLTR","SHOP","PATH","CSCO","AVGO","AMD","INTC","NFLX","SPOT",
    "TSLA","COST","WMT","TGT","BABA","SONY","TTWO","JPM","BRK-B","BLK",
    "HOOD","IFC.TO","TMUS","CVX","DVN","URA","UEC","CCO.TO",
    "UUUU","MP","USAR","ALB","LAC","AG","MSTR","IREN","BMNR","APLD",
    "AVAV","RR.L","XPEV","RIVN","ONDS","SRFM","CCO","MGY"
]
_day_mod = datetime.utcnow().day % 3
_av_batch_start = _day_mod * 18
_av_batch_today = set(_ALL_TICKERS_ORDERED[_av_batch_start:_av_batch_start + 18])


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


# All stocks split into two batches — alternate each day to stay within AV free tier (25/day)
# Batch A = odd days, Batch B = even days. Each batch ≤ 27 stocks.
AV_BATCH_A = [
    "MSFT","AMZN","AAPL","NVDA","META","GOOG","NFLX","TSLA",
    "ORCL","PLTR","SHOP","AVGO","AMD","INTC","JPM","BLK",
    "COST","WMT","IBM","ADBE","CSCO","CVX","DVN","TMUS",
    "MSTR","URA","CCO"
]
AV_BATCH_B = [
    "NFLX","TGT","BABA","SONY","TTWO","HOOD","IFC.TO","SPOT",
    "UEC","CCO.TO","UUUU","MP","ALB","LAC","AG","AVAV","RR.L",
    "XPEV","RIVN","IREN","BMNR","APLD","ONDS","SRFM","USAR","MGY","PATH"
]

# Today's batch — odd day = A, even day = B
_today_batch = set(AV_BATCH_A if datetime.utcnow().day % 2 == 1 else AV_BATCH_B)

def fetch_fundamentals(ticker):
    """
    Fetch fundamentals via Alpha Vantage OVERVIEW.
    Rotates through all 54 stocks in 3 daily batches of 18
    to stay within the 25 calls/day free tier limit.
    Stocks not in today's batch reuse the cached values from data.json.
    """
    result = {
        "trailing_pe": None, "forward_pe": None, "market_cap": None,
        "week52_high": None, "week52_low": None, "ps_ratio": None,
        "eps_ttm": None, "beta": None, "analyst_target": None,
        "industry": None, "earnings_history": [],
    }

    if not AV_KEY:
        print(f"  ⚠ No AV key — skipping fundamentals for {ticker}")
        return result

    av_ticker = (ticker
        .replace(".TO","").replace(".L","")
        .replace("BRK-B","BRK.B").replace("CCO.TO","CCO"))

    if ticker not in _av_batch_today:
        print(f"  ℹ {ticker} not in today's AV batch (day mod 3 = {_day_mod}) — using cached")
        return None  # None = caller should keep existing cached values

    url = (f"https://www.alphavantage.co/query?function=OVERVIEW"
           f"&symbol={av_ticker}&apikey={AV_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            ov = json.loads(r.read())

        if "Information" in ov or "Note" in ov:
            print(f"  ⚠ AV rate limit: {ov.get('Information') or ov.get('Note','')[:80]}")
            return result

        if not ov.get("Symbol"):
            print(f"  ℹ AV: no overview for {av_ticker} (ETF/unlisted)")
            return result

        def av_float(k):
            v = ov.get(k)
            try:
                return float(v) if v and v not in ("None","-","0","") else None
            except (ValueError, TypeError):
                return None

        mc = ov.get("MarketCapitalization","0")
        result.update({
            "trailing_pe":    av_float("TrailingPE"),
            "forward_pe":     av_float("ForwardPE"),
            "market_cap":     int(mc) if mc and mc not in ("None","-","0","") else None,
            "week52_high":    av_float("52WeekHigh"),
            "week52_low":     av_float("52WeekLow"),
            "ps_ratio":       av_float("PriceToSalesRatioTTM"),
            "eps_ttm":        av_float("DilutedEPSTTM"),
            "beta":           av_float("Beta"),
            "analyst_target": av_float("AnalystTargetPrice"),
            "industry":       ov.get("Industry","") or None,
        })
        print(f"  ✅ AV Overview: PE={result['trailing_pe']} Cap={result['market_cap']} Beta={result['beta']}")

    except Exception as e:
        print(f"  ⚠ AV Overview failed for {ticker}: {e}")

    time.sleep(0.5)
    return result


def fetch_eps_history_av(ticker):
    """
    Fetch quarterly EPS history from Alpha Vantage.
    Returns list of {"quarter": {"raw": timestamp}, "epsActual": {"raw": float}}
    Compatible with calculate_pe_history() format.
    """
    if not AV_KEY:
        return []
    # Alpha Vantage uses different ticker format for some stocks
    av_ticker = ticker.replace(".TO", "").replace(".L", ".LON").replace("BRK-B", "BRK.B")
    url = (f"https://www.alphavantage.co/query?function=EARNINGS"
           f"&symbol={av_ticker}&apikey={AV_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())

        quarterly = data.get("quarterlyEarnings", [])
        if not quarterly:
            print(f"  ℹ AV: no quarterly earnings for {ticker}")
            return []

        eh = []
        for q in quarterly:
            try:
                # AV date format: "2024-03-31"
                q_date = date.fromisoformat(q["fiscalDateEnding"])
                eps    = float(q.get("reportedEPS", 0) or 0)
                if eps == 0:
                    continue
                eh.append({
                    "quarter":   {"raw": int(datetime.combine(q_date, datetime.min.time()).timestamp())},
                    "epsActual": {"raw": eps},
                })
            except Exception:
                continue

        eh.sort(key=lambda x: x["quarter"]["raw"])
        print(f"  ✅ AV earnings: {len(eh)} quarters for {ticker}")
        return eh

    except Exception as e:
        print(f"  ⚠ AV earnings failed for {ticker}: {e}")
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
def calculate_risk(history, inception_str, ticker_hint=""):
    inception = date.fromisoformat(inception_str)
    data = [(d, p) for d, p in history if d >= inception and p > 0]
    if len(data) < 10: return None

    days_history = (data[-1][0] - data[0][0]).days
    prices       = [p for _, p in data]

    def days_since(d): return (d - inception).days

    full_risks = []

    # Determine method — check override first, then history length
    forced_method = METHOD_OVERRIDES.get(ticker_hint, None)
    if forced_method:
        use_method = forced_method
    elif days_history >= STRUCTURAL_MIN_DAYS:
        use_method = "Structural"
    elif days_history >= TECHNICAL_MIN_DAYS:
        use_method = "Technical"
    else:
        use_method = "Early Stage"

    if use_method == "Structural":
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

    elif use_method == "Technical":
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
        method = "Early Stage"  # use_method == "Early Stage"
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

    # ── Buy Score (0–1, higher = stronger buy signal) ──────────────────────
    # Blend: RSI inverted (25%) + BB inverted (25%) + 52wk drawdown (20%)
    #        + deviation below fair value (15%) + analyst upside (15%)
    def compute_buy_score(price, rsi, bb_pos, fair_val, dev, week52_hi, analyst_tgt):
        components = []
        # RSI component — oversold = high buy score
        rsi_score = max(0, min(1, (100 - rsi) / 100)) if rsi is not None else 0.5
        components.append(('rsi', rsi_score, 0.25))
        # Bollinger component — near lower band = high buy score
        bb_score = max(0, min(1, 1 - bb_pos)) if bb_pos is not None else 0.5
        components.append(('bb', bb_score, 0.25))
        # 52-week drawdown — far below 52w high = high buy score
        if week52_hi and week52_hi > 0 and price:
            ddown = max(0, min(1, (week52_hi - price) / week52_hi))
        else:
            ddown = 0.3  # neutral default
        components.append(('drawdown', ddown, 0.20))
        # Deviation below fair value — undervalued = high buy score
        if dev is not None:
            dev_score = max(0, min(1, 0.5 - dev))  # dev<0 means below FV → score>0.5
        else:
            dev_score = 0.5
        components.append(('deviation', dev_score, 0.15))
        # Analyst upside — large upside to target = high buy score
        if analyst_tgt and price and price > 0:
            upside = (analyst_tgt - price) / price
            upside_score = max(0, min(1, 0.5 + upside))
        else:
            upside_score = 0.5
        components.append(('analyst', upside_score, 0.15))
        # Weighted sum
        total_w = sum(w for _, _, w in components)
        score   = sum(s * w for _, s, w in components) / total_w
        return round(max(0.0, min(1.0, score)), 4)

    # Rolling buy score for chart history
    buy_score_history = []
    for i in range(len(data)):
        p_i    = data[i][1]
        rsi_i  = calculate_rsi(prices[:i+1]) if i >= 14 else rsi_display
        bb_i   = calculate_bb_position(prices[:i+1]) if i >= 20 else bb_display
        ri     = full_risks[i] if i < len(full_risks) else current_risk
        fv_i   = None  # fair value changes over time but we skip rolling FV for perf
        dev_i  = None
        if method == "Structural" and i < len(xs):
            fv_log = slope * xs[i] + intercept
            fv_i   = 10 ** fv_log
            dev_i  = (p_i - fv_i) / fv_i if fv_i else None
        bs = compute_buy_score(p_i, rsi_i, bb_i, fv_i, dev_i, None, None)
        buy_score_history.append(round(bs, 4))

    # Current buy score with full data
    current_buy_score = compute_buy_score(
        last_p, rsi_display, bb_display, fair_value, deviation,
        locals().get('week52_hi_val'), locals().get('analyst_tgt_val')
    )

    # ── Chart data — full history downsampled to ~500 points ─────────────
    chart_data_raw = [(d, p, full_risks[i], buy_score_history[i])
                      for i, (d, p) in enumerate(data)]

    # Weekly downsample to ~500 points max
    if len(chart_data_raw) > 500:
        step = max(1, len(chart_data_raw) // 500)
        chart_data_raw = chart_data_raw[::step]
        # Always include the last point
        if chart_data_raw[-1] != (data[-1][0], data[-1][1], full_risks[-1], buy_score_history[-1]):
            chart_data_raw.append((data[-1][0], data[-1][1], full_risks[-1], buy_score_history[-1]))

    chart = {
        "dates":       [d.isoformat() for d, _, _, _ in chart_data_raw],
        "prices":      [round(p, 2)    for _, p, _, _ in chart_data_raw],
        "risks":       [round(r, 4)    for _, _, r, _ in chart_data_raw],
        "buy_scores":  [round(b, 4)    for _, _, _, b in chart_data_raw],
    }

    def buy_zone(b):
        if b >= 0.65: return "STRONG BUY"
        if b >= 0.50: return "BUY"
        if b >= 0.35: return "NEUTRAL"
        if b >= 0.20: return "WEAK"
        return "AVOID"

    return {
        "price":          round(last_p, 2),
        "risk":           round(current_risk, 4),
        "risk_method":    method,
        "zone":           risk_zone(current_risk),
        "buy_score":      current_buy_score,
        "buy_zone":       buy_zone(current_buy_score),
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
    print(f"AV Batch: day mod 3 = {_day_mod} — {len(_av_batch_today)} stocks refreshing fundamentals today")
    print(f"Batch tickers: {sorted(_av_batch_today)}")
    print(f"{'='*60}\n")

    # Load existing data.json to preserve cached fundamentals for stocks
    # not in today's AV batch
    cached = {}
    try:
        with open("data.json") as f:
            old = json.load(f)
            for t, s in old.get("stocks", {}).items():
                cached[t] = {k: s.get(k) for k in (
                    "trailing_pe","forward_pe","market_cap","week52_high",
                    "week52_low","ps_ratio","eps_ttm","beta","analyst_target",
                    "industry","pe_history","earnings_history"
                )}
        print(f"Loaded cached fundamentals for {len(cached)} stocks")
    except Exception:
        print("No existing data.json — fresh run")

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
        fund_result  = fetch_fundamentals(ticker); time.sleep(0.3)
        news         = fetch_news(ticker);         time.sleep(0.2)
        # None = not in today's batch, use cached values
        if fund_result is None:
            fundamentals = cached.get(ticker, {})
        else:
            fundamentals = fund_result

        if not history:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "sector": sector,
                "error": "No price data",
            }
            output["summary"]["unknown"] += 1
            continue

        risk_data = calculate_risk(history, cfg["inception"], ticker)
        # Inject fundamentals into buy score after the fact
        if risk_data and fundamentals:
            w52h = fundamentals.get("week52_high")
            atgt = fundamentals.get("analyst_target")
            if w52h or atgt:
                import types
                # Recompute current buy score with full data
                def _buy_score(price, rsi, bb, fv, dev, wh, at):
                    c = []
                    c.append(max(0,min(1,(100-(rsi or 50))/100)) * 0.25)
                    c.append(max(0,min(1,1-(bb or 0.5))) * 0.25)
                    dd = max(0,min(1,(wh-price)/wh)) if wh and wh>0 and price else 0.3
                    c.append(dd * 0.20)
                    ds = max(0,min(1,0.5-(dev or 0))) * 0.15
                    c.append(ds)
                    us = max(0,min(1,0.5+((at-price)/price))) if at and price>0 else 0.5
                    c.append(us * 0.15)
                    return round(sum(c), 4)
                risk_data["buy_score"] = _buy_score(
                    risk_data["price"], risk_data["rsi"], risk_data["bb_position"],
                    risk_data["fair_value"], risk_data["deviation"], w52h, atgt
                )
        if not risk_data:
            output["stocks"][ticker] = {
                "name": cfg["name"], "ticker": ticker,
                "allocation": cfg["allocation"], "sector": sector,
                "error": "Insufficient history for risk calculation",
            }
            output["summary"]["unknown"] += 1
            continue

        # Use cached pe_history if not refreshed today
        if fund_result is None and cached.get(ticker, {}).get("pe_history"):
            pe_hist = cached[ticker]["pe_history"]
        else:
            pe_hist = calculate_pe_history(history, fundamentals)

        def v(k): return fundamentals.get(k)

        output["stocks"][ticker] = {
            "name":           cfg["name"],
            "ticker":         ticker,
            "allocation":     cfg["allocation"],
            "sector":         sector,
            "trailing_pe":    v("trailing_pe"),
            "forward_pe":     v("forward_pe"),
            "market_cap":     v("market_cap"),
            "week52_high":    v("week52_high"),
            "week52_low":     v("week52_low"),
            "ps_ratio":       v("ps_ratio"),
            "beta":           v("beta"),
            "analyst_target": v("analyst_target"),
            "industry":       v("industry"),
            "pe_history":     pe_hist,
            "news":           news,
            "last_updated":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
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
