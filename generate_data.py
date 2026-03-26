"""
Portfolio Risk Dashboard v2 — Comprehensive Data Generator
- Three risk calculation methods based on history length
- Fundamentals: P/E, market cap, 52-week high/low
- Historical P/E chart from earnings history
- Multi-year returns (1-5 year)
- RSI + Bollinger Band calculations
- Sector grouping + correlation data
- News headlines
- Buy score: RSI + Bollinger + 52wk drawdown + trend deviation + analyst upside
- Analyst target: Yahoo Finance (free, no key)
- Full history chart weekly downsampled max 500 pts
"""

import json, math, os, subprocess, time
from datetime import date, datetime, timedelta
import urllib.request, urllib.error

AV_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")

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
    "MGY":  "Energy",           "URA":  "Uranium / Nuclear","UEC": "Uranium / Nuclear",
    "CCO.TO":"Uranium / Nuclear","UUUU":"Uranium / Nuclear","MP":  "Rare Earth",
    "USAR": "Rare Earth",       "ALB":  "Materials",       "LAC":  "Materials",
    "AG":   "Materials",        "MSTR": "Crypto / Bitcoin","IREN": "Crypto / Bitcoin",
    "BMNR": "Crypto / Bitcoin", "APLD": "Crypto / Bitcoin","AVAV": "Defence",
    "RR.L": "Defence",          "XPEV": "EV / Auto",       "RIVN": "EV / Auto",
    "ONDS": "Small Cap",        "SRFM": "Small Cap",       "CCO":  "Media",
}

STOCK_LIST = {
    "MSFT": {"name": "Microsoft",             "inception": "1986-03-13", "allocation": 3},
    "AMZN": {"name": "Amazon",                "inception": "1997-05-15", "allocation": 3},
    "NVDA": {"name": "Nvidia",                "inception": "1999-01-22", "allocation": 3},
    "META": {"name": "Meta Platforms",        "inception": "2012-05-18", "allocation": 3},
    "AAPL": {"name": "Apple",                 "inception": "1980-12-12", "allocation": 3},
    "GOOG": {"name": "Alphabet",              "inception": "2004-08-19", "allocation": 3},
    "IBM":  {"name": "IBM",                   "inception": "1962-01-02", "allocation": 2},
    "ADBE": {"name": "Adobe",                 "inception": "1986-08-13", "allocation": 2},
    "ORCL": {"name": "Oracle",                "inception": "1986-03-12", "allocation": 2},
    "PLTR": {"name": "Palantir",              "inception": "2020-09-30", "allocation": 2},
    "SHOP": {"name": "Shopify",               "inception": "2015-05-21", "allocation": 2},
    "PATH": {"name": "UiPath",                "inception": "2021-04-21", "allocation": 2},
    "CSCO": {"name": "Cisco",                 "inception": "1990-02-16", "allocation": 2},
    "AVGO": {"name": "Broadcom",              "inception": "2009-08-06", "allocation": 2},
    "AMD":  {"name": "AMD",                   "inception": "1972-09-27", "allocation": 2},
    "INTC": {"name": "Intel",                 "inception": "1971-10-13", "allocation": 2},
    "NFLX": {"name": "Netflix",               "inception": "2002-05-23", "allocation": 2},
    "SPOT": {"name": "Spotify",               "inception": "2018-04-03", "allocation": 2},
    "TSLA": {"name": "Tesla",                 "inception": "2010-06-29", "allocation": 2},
    "COST": {"name": "Costco",                "inception": "1985-12-05", "allocation": 2},
    "WMT":  {"name": "Walmart",               "inception": "1972-08-25", "allocation": 2},
    "TGT":  {"name": "Target",                "inception": "1967-01-03", "allocation": 2},
    "BABA": {"name": "Alibaba",               "inception": "2014-09-19", "allocation": 2},
    "SONY": {"name": "Sony Group",            "inception": "1970-09-01", "allocation": 2},
    "TTWO": {"name": "Take-Two Interactive",  "inception": "1993-09-24", "allocation": 2},
    "JPM":  {"name": "JPMorgan Chase",        "inception": "1969-01-02", "allocation": 2},
    "BRK-B":{"name": "Berkshire Hathaway B",  "inception": "1996-05-09", "allocation": 2},
    "BLK":  {"name": "BlackRock",             "inception": "1999-10-01", "allocation": 2},
    "HOOD": {"name": "Robinhood",             "inception": "2021-07-29", "allocation": 2},
    "IFC.TO":{"name":"Intact Financial",      "inception": "2004-02-12", "allocation": 2},
    "TMUS": {"name": "T-Mobile US",           "inception": "2013-05-01", "allocation": 2},
    "CVX":  {"name": "Chevron",               "inception": "1970-01-02", "allocation": 2},
    "DVN":  {"name": "Devon Energy",          "inception": "1988-01-04", "allocation": 2},
    "MGY":  {"name": "Magnolia Oil & Gas",    "inception": "2018-07-19", "allocation": 2},
    "URA":  {"name": "Global X Uranium ETF",  "inception": "2010-11-04", "allocation": 2},
    "UEC":  {"name": "Uranium Energy Corp",   "inception": "2007-01-03", "allocation": 2},
    "CCO.TO":{"name":"Cameco",                "inception": "1991-01-02", "allocation": 2},
    "UUUU": {"name": "Energy Fuels",          "inception": "2012-01-03", "allocation": 2},
    "MP":   {"name": "MP Materials",          "inception": "2020-11-17", "allocation": 2},
    "USAR": {"name": "USA Rare Earth",        "inception": "2024-01-02", "allocation": 2},
    "ALB":  {"name": "Albemarle",             "inception": "1994-02-28", "allocation": 2},
    "LAC":  {"name": "Lithium Americas",      "inception": "2020-09-01", "allocation": 2},
    "AG":   {"name": "First Majestic Silver", "inception": "2011-05-10", "allocation": 2},
    "MSTR": {"name": "MicroStrategy",         "inception": "2020-08-11", "allocation": 2},
    "IREN": {"name": "Iris Energy",           "inception": "2021-11-18", "allocation": 2},
    "BMNR": {"name": "Bitmine Immersion",     "inception": "2022-06-01", "allocation": 2},
    "APLD": {"name": "Applied Digital",       "inception": "2022-04-01", "allocation": 2},
    "AVAV": {"name": "AeroVironment",         "inception": "2007-01-26", "allocation": 2},
    "RR.L": {"name": "Rolls-Royce",           "inception": "1987-01-01", "allocation": 2},
    "XPEV": {"name": "XPeng",                 "inception": "2020-08-27", "allocation": 2},
    "RIVN": {"name": "Rivian",                "inception": "2021-11-10", "allocation": 2},
    "ONDS": {"name": "Ondas Holdings",        "inception": "2018-01-02", "allocation": 1},
    "SRFM": {"name": "Surf Air Mobility",     "inception": "2023-07-27", "allocation": 1},
    "CCO":  {"name": "Clear Channel Outdoor", "inception": "2005-11-11", "allocation": 1},
}

STRUCTURAL_MIN_DAYS = 1825
TECHNICAL_MIN_DAYS  = 730
SIGMA_LOWER = 2.0
SIGMA_UPPER = 2.0

METHOD_OVERRIDES = {
    "INTC": "Technical",
    "SONY": "Technical",
    "IBM":  "Technical",
    "BABA": "Technical",
    "CCO":  "Technical",
    "MSTR": "Technical",
}

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


def yahoo_get(url, timeout=20):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; portfolio-dashboard/2.0)"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_history(ticker):
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=max"
    try:
        data   = yahoo_get(url)
        result = data["chart"]["result"][0]
        ts     = result["timestamp"]
        closes = result["indicators"]["quote"][0]["close"]
        pts    = [(date.fromtimestamp(t), c) for t, c in zip(ts, closes) if c and c > 0]
        return sorted(pts)
    except Exception as e:
        print(f"  WARNING History failed for {ticker}: {e}")
        return []


def fetch_news(ticker):
    url = f"https://query2.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=3&enableFuzzyQuery=false"
    try:
        data  = yahoo_get(url)
        items = data.get("news", [])
        return [{"title": i.get("title",""), "url": i.get("link",""),
                 "publisher": i.get("publisher",""), "date": i.get("providerPublishTime",0)}
                for i in items[:3]]
    except Exception as e:
        print(f"  WARNING News failed for {ticker}: {e}")
        return []


def fetch_analyst_target_yahoo(ticker):
    """Fetch analyst mean target price from Yahoo Finance quoteSummary (free, no API key)."""
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData"
    try:
        data   = yahoo_get(url)
        result = data.get("quoteSummary", {}).get("result", [])
        if not result:
            return None
        fd     = result[0].get("financialData", {})
        target = fd.get("targetMeanPrice", {})
        val    = target.get("raw") if isinstance(target, dict) else None
        if val and float(val) > 0:
            print(f"  OK Analyst target (Yahoo): ${float(val):.2f}")
            return round(float(val), 2)
        return None
    except Exception as e:
        print(f"  WARNING Analyst target failed for {ticker}: {e}")
        return None


def fetch_fundamentals(ticker):
    result = {
        "trailing_pe": None, "forward_pe": None, "market_cap": None,
        "week52_high": None, "week52_low": None, "ps_ratio": None,
        "eps_ttm": None, "beta": None, "industry": None, "earnings_history": [],
    }
    if not AV_KEY:
        print(f"  WARNING No AV key for {ticker}")
        return result

    av_ticker = (ticker.replace(".TO","").replace(".L","")
                       .replace("BRK-B","BRK.B").replace("CCO.TO","CCO"))

    if ticker not in _av_batch_today:
        print(f"  INFO {ticker} not in today's AV batch (day mod 3 = {_day_mod}) -- using cached")
        return None

    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={av_ticker}&apikey={AV_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            ov = json.loads(r.read())
        if "Information" in ov or "Note" in ov:
            print(f"  WARNING AV rate limit: {ov.get('Information') or ov.get('Note','')[:80]}")
            return result
        if not ov.get("Symbol"):
            print(f"  INFO AV: no overview for {av_ticker}")
            return result

        def av_float(k):
            v = ov.get(k)
            try:   return float(v) if v and v not in ("None","-","0","") else None
            except: return None

        mc = ov.get("MarketCapitalization","0")
        result.update({
            "trailing_pe": av_float("TrailingPE"),
            "forward_pe":  av_float("ForwardPE"),
            "market_cap":  int(mc) if mc and mc not in ("None","-","0","") else None,
            "week52_high": av_float("52WeekHigh"),
            "week52_low":  av_float("52WeekLow"),
            "ps_ratio":    av_float("PriceToSalesRatioTTM"),
            "eps_ttm":     av_float("DilutedEPSTTM"),
            "beta":        av_float("Beta"),
            "industry":    ov.get("Industry","") or None,
        })
        print(f"  OK AV Overview: PE={result['trailing_pe']} Cap={result['market_cap']}")
    except Exception as e:
        print(f"  WARNING AV Overview failed for {ticker}: {e}")
    time.sleep(0.5)
    return result


def fetch_eps_history_av(ticker):
    if not AV_KEY:
        return []
    av_ticker = ticker.replace(".TO","").replace(".L",".LON").replace("BRK-B","BRK.B")
    url = f"https://www.alphavantage.co/query?function=EARNINGS&symbol={av_ticker}&apikey={AV_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        quarterly = data.get("quarterlyEarnings", [])
        if not quarterly: return []
        eh = []
        for q in quarterly:
            try:
                q_date = date.fromisoformat(q["fiscalDateEnding"])
                eps    = float(q.get("reportedEPS", 0) or 0)
                if eps == 0: continue
                eh.append({
                    "quarter":   {"raw": int(datetime.combine(q_date, datetime.min.time()).timestamp())},
                    "epsActual": {"raw": eps},
                })
            except Exception:
                continue
        eh.sort(key=lambda x: x["quarter"]["raw"])
        print(f"  OK AV earnings: {len(eh)} quarters for {ticker}")
        return eh
    except Exception as e:
        print(f"  WARNING AV earnings failed for {ticker}: {e}")
        return []


def linear_regression(xs, ys):
    n = len(xs)
    if n < 10: return 0, 0
    sx  = sum(xs); sy  = sum(ys)
    sxy = sum(x*y for x, y in zip(xs, ys))
    sx2 = sum(x*x for x in xs)
    d   = n*sx2 - sx*sx
    if d == 0: return 0, 0
    slope = (n*sxy - sx*sy) / d
    return slope, (sy - slope*sx) / n


def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        chg = prices[i] - prices[i-1]
        gains.append(max(0, chg)); losses.append(max(0, -chg))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag*(period-1) + gains[i]) / period
        al = (al*(period-1) + losses[i]) / period
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


def calculate_buy_score_current(rsi, bb_position, trend_deviation,
                                 week52_high, price, analyst_target):
    """
    Point-in-time buy score 0-1 (1 = strongest buy signal).
    Weights: RSI 25% | Bollinger 25% | 52wk drawdown 20% | Trend deviation 15% | Analyst upside 15%
    """
    # RSI: oversold (low) = high buy score
    rsi_score = max(0.0, min(1.0, (70.0 - rsi) / 70.0))

    # Bollinger: near lower band = high buy score
    bb_score = 1.0 - bb_position

    # 52-week drawdown from high
    if week52_high and price and week52_high > 0:
        w52_score = min(1.0, max(0.0, (week52_high - price) / week52_high))
    else:
        w52_score = 0.5

    # Trend deviation: negative (below trend) = bullish
    if trend_deviation is not None:
        dev_score = max(0.0, min(1.0, 0.5 - trend_deviation))
    else:
        dev_score = 0.5

    # Analyst upside
    if analyst_target and price and price > 0:
        upside = (analyst_target - price) / price
        analyst_score = max(0.0, min(1.0, upside + 0.5))
    else:
        analyst_score = 0.5

    score = (0.25 * rsi_score +
             0.25 * bb_score  +
             0.20 * w52_score +
             0.15 * dev_score +
             0.15 * analyst_score)
    return round(max(0.0, min(1.0, score)), 4)


def calculate_risk(history, inception_str, ticker_hint=""):
    inception = date.fromisoformat(inception_str)
    data      = [(d, p) for d, p in history if d >= inception and p > 0]
    if len(data) < 10: return None

    days_history    = (data[-1][0] - data[0][0]).days
    prices          = [p for _, p in data]
    full_risks      = []
    full_buys       = []
    trend_value     = None
    trend_deviation = None
    risk_price_map  = None

    def days_since(d): return (d - inception).days

    forced_method = METHOD_OVERRIDES.get(ticker_hint)
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
        xs           = [days_since(d) for d, _ in data]
        ys           = [math.log10(p)  for _, p  in data]
        slope, intercept = linear_regression(xs, ys)
        predicted    = [slope*x + intercept for x in xs]
        residuals    = [y - p for y, p in zip(ys, predicted)]
        std_full     = (sum(r**2 for r in residuals) / len(residuals)) ** 0.5
        lower_band   = -SIGMA_LOWER * std_full
        upper_band   =  SIGMA_UPPER * std_full
        band_range   = upper_band - lower_band
        if band_range == 0: return None

        full_risks = [max(0.0, min(1.0, (r - lower_band) / band_range)) for r in residuals]
        current_risk    = full_risks[-1]
        last_x          = xs[-1]
        trend_value     = round(10 ** (slope * last_x + intercept), 2)
        trend_deviation = round((prices[-1] - trend_value) / trend_value, 4)

        risk_price_map = {}
        for rv in [i/20 for i in range(21)]:
            target_log = (slope * last_x + intercept) + (lower_band + rv * band_range)
            risk_price_map[round(rv, 3)] = round(10 ** target_log, 2)

        # Rolling buy score for chart
        for i, r in enumerate(residuals):
            sl    = prices[max(0, i-60):i+1]
            rsi_i = calculate_rsi(sl) / 100.0
            bb_i  = calculate_bb_position(sl)
            risk_i = max(0.0, min(1.0, (r - lower_band) / band_range))
            dev_score = max(0.0, min(1.0, 0.5 - risk_i * 0.5))
            buy_i = 0.30 * (1.0 - rsi_i) + 0.30 * (1.0 - bb_i) + 0.40 * dev_score
            full_buys.append(round(max(0.0, min(1.0, buy_i)), 4))

    elif use_method == "Technical":
        method = "Technical"
        rsi_val = calculate_rsi(prices)
        bb_val  = calculate_bb_position(prices)
        current_risk = 0.5 * (rsi_val / 100) + 0.5 * bb_val

        for i in range(len(prices)):
            sl    = prices[max(0, i-60):i+1]
            rsi_i = calculate_rsi(sl) / 100.0
            bb_i  = calculate_bb_position(sl)
            full_risks.append(round(0.5 * rsi_i + 0.5 * bb_i, 4))
            full_buys.append(round(max(0.0, min(1.0,
                0.5 * (1.0 - rsi_i) + 0.5 * (1.0 - bb_i))), 4))

    else:
        method = "Early Stage"
        mn_p, mx_p = min(prices), max(prices)
        current_risk = (prices[-1] - mn_p) / (mx_p - mn_p) if mx_p != mn_p else 0.5

        for i in range(len(prices)):
            sl     = prices[:i+1]
            mn2, mx2 = min(sl), max(sl)
            risk_i = (sl[-1] - mn2) / (mx2 - mn2) if mx2 != mn2 else 0.5
            sub    = prices[max(0, i-60):i+1]
            rsi_i  = calculate_rsi(sub) / 100.0
            bb_i   = calculate_bb_position(sub)
            full_risks.append(risk_i)
            full_buys.append(round(max(0.0, min(1.0,
                0.4 * (1-risk_i) + 0.3 * (1-rsi_i) + 0.3 * (1-bb_i))), 4))

    rsi_display = calculate_rsi(prices)
    bb_display  = calculate_bb_position(prices)

    returns = {}
    last_d, last_p = data[-1]
    for yrs in [1, 2, 3, 4, 5]:
        target     = date(last_d.year - yrs, last_d.month, last_d.day)
        candidates = [(abs((d - target).days), p) for d, p in data if abs((d-target).days) < 30]
        if candidates:
            _, past_p = min(candidates)
            returns[f"ret_{yrs}y"] = round((last_p - past_p) / past_p, 4)
        else:
            returns[f"ret_{yrs}y"] = None

    hist_risk_scores = []
    for yr_offset in range(1, 6):
        target     = date(last_d.year - yr_offset, last_d.month, last_d.day)
        candidates = [(abs((d - target).days), i) for i, (d, _) in enumerate(data)
                      if abs((d-target).days) < 30]
        if candidates and full_risks:
            _, idx = min(candidates)
            if idx < len(full_risks):
                hist_risk_scores.append({"year": target.year, "risk": round(full_risks[idx], 4)})

    # Full history chart — weekly downsampled, max 500 points
    n_pts   = len(data)
    step    = max(1, n_pts // 500)
    indices = list(range(0, n_pts, step))
    if (n_pts - 1) not in indices:
        indices.append(n_pts - 1)

    chart = {
        "dates":  [data[i][0].isoformat()    for i in indices],
        "prices": [round(data[i][1], 2)      for i in indices],
        "risks":  [round(full_risks[i], 4)   for i in indices] if len(full_risks) == n_pts else [],
        "buys":   [round(full_buys[i], 4)    for i in indices] if len(full_buys)  == n_pts else [],
    }

    return {
        "price":           round(last_p, 2),
        "risk":            round(current_risk, 4),
        "risk_method":     method,
        "zone":            risk_zone(current_risk),
        "trend_value":     trend_value,
        "trend_deviation": trend_deviation,
        "rsi":             round(rsi_display, 1),
        "bb_position":     round(bb_display, 3),
        "hist_risks":      hist_risk_scores,
        "risk_price_map":  risk_price_map,
        "chart":           chart,
        **returns,
    }


def calculate_pe_history(history, fundamentals):
    earnings_history = fundamentals.get("earnings_history", [])
    eps_ttm          = fundamentals.get("eps_ttm")

    def get_raw(d, k):
        v = d.get(k)
        return v.get("raw") if isinstance(v, dict) else v

    pts = []
    if earnings_history and len(earnings_history) >= 4:
        try:
            eh = sorted(earnings_history, key=lambda x: get_raw(x,"quarter") or 0)
            for i in range(3, len(eh)):
                q_ts = get_raw(eh[i], "quarter")
                if not q_ts: continue
                q_date   = date.fromtimestamp(q_ts)
                eps_vals = []
                for j in range(4):
                    e = get_raw(eh[i-j], "epsActual")
                    if e is not None: eps_vals.append(e)
                if len(eps_vals) < 4: continue
                ttm = sum(eps_vals)
                if ttm <= 0: continue
                nearby = [(abs((d2 - q_date).days), p) for d2, p in history
                          if abs((d2-q_date).days) < 25]
                if not nearby: continue
                _, price = min(nearby)
                pe = price / ttm
                if 0 < pe < 1000:
                    pts.append({"date": q_date.isoformat(), "pe": round(pe, 1)})
        except Exception as e:
            print(f"  WARNING PE history failed: {e}")

    if not pts and eps_ttm and eps_ttm > 0 and history:
        last_price = history[-1][1]
        pe = last_price / eps_ttm
        if 0 < pe < 1000:
            pts.append({"date": history[-1][0].isoformat(), "pe": round(pe, 1)})

    return pts[-20:]


def main():
    print(f"\n{'='*60}")
    print(f"Portfolio Dashboard v2 -- Data Update")
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"AV Batch: day mod 3 = {_day_mod} -- {len(_av_batch_today)} stocks refreshing today")
    print(f"Batch tickers: {sorted(_av_batch_today)}")
    print(f"{'='*60}\n")

    cached = {}
    try:
        with open("data.json") as f:
            old = json.load(f)
        for t, s in old.get("stocks", {}).items():
            cached[t] = {k: s.get(k) for k in (
                "trailing_pe","forward_pe","market_cap","week52_high",
                "week52_low","ps_ratio","eps_ttm","beta",
                "industry","pe_history","earnings_history"
            )}
        print(f"Loaded cached fundamentals for {len(cached)} stocks")
    except Exception:
        print("No existing data.json -- fresh run")

    output = {
        "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stocks":    {},
        "summary":   {"accumulate":0,"neutral":0,"caution":0,"extreme":0,"unknown":0},
        "sectors":   {},
    }

    for ticker, cfg in STOCK_LIST.items():
        print(f"\n[{ticker}] {cfg['name']}")
        sector = SECTORS.get(ticker, "Other")

        history     = fetch_history(ticker);              time.sleep(0.3)
        fund_result = fetch_fundamentals(ticker);         time.sleep(0.3)
        analyst_tgt = fetch_analyst_target_yahoo(ticker); time.sleep(0.2)
        news        = fetch_news(ticker);                 time.sleep(0.2)

        if fund_result is None:
            fundamentals = cached.get(ticker, {})
        else:
            fundamentals = fund_result

        if not history:
            output["stocks"][ticker] = {
                "name":cfg["name"],"ticker":ticker,
                "allocation":cfg["allocation"],"sector":sector,
                "error":"No price data",
            }
            output["summary"]["unknown"] += 1
            continue

        risk_data = calculate_risk(history, cfg["inception"], ticker)
        if not risk_data:
            output["stocks"][ticker] = {
                "name":cfg["name"],"ticker":ticker,
                "allocation":cfg["allocation"],"sector":sector,
                "error":"Insufficient history",
            }
            output["summary"]["unknown"] += 1
            continue

        if fund_result is None and cached.get(ticker, {}).get("pe_history"):
            pe_hist = cached[ticker]["pe_history"]
        else:
            pe_hist = calculate_pe_history(history, fundamentals)

        buy_score = calculate_buy_score_current(
            rsi             = risk_data["rsi"],
            bb_position     = risk_data["bb_position"],
            trend_deviation = risk_data.get("trend_deviation"),
            week52_high     = fundamentals.get("week52_high"),
            price           = risk_data["price"],
            analyst_target  = analyst_tgt,
        )

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
            "analyst_target": analyst_tgt,
            "industry":       v("industry"),
            "pe_history":     pe_hist,
            "buy_score":      buy_score,
            "news":           news,
            "last_updated":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            **risk_data,
        }

        zone = risk_data["zone"].lower()
        output["summary"][zone] = output["summary"].get(zone, 0) + 1

        if sector not in output["sectors"]:
            output["sectors"][sector] = {"tickers":[], "avg_risk":None}
        output["sectors"][sector]["tickers"].append(ticker)

        print(f"  ${risk_data['price']:>10.2f} | Risk {risk_data['risk']:.3f} "
              f"| Buy {buy_score:.3f} | {risk_data['zone']:<12} | {risk_data['risk_method']}")

    for sector, sdata in output["sectors"].items():
        valid = [output["stocks"][t]["risk"] for t in sdata["tickers"]
                 if output["stocks"].get(t,{}).get("risk") is not None]
        if valid:
            output["sectors"][sector]["avg_risk"] = round(sum(valid)/len(valid), 4)

    with open("data.json", "w") as f:
        json.dump(output, f, separators=(",",":"))
    print(f"\nOK data.json written ({len(json.dumps(output))//1024} KB)")

    if not os.path.exists("ideas.json"):
        with open("ideas.json", "w") as f:
            json.dump({"ideas":[]}, f)

    try:
        subprocess.run(["git","config","user.name","Dashboard Bot"],  check=True)
        subprocess.run(["git","config","user.email","bot@users.noreply.github.com"], check=True)
        subprocess.run(["git","add","data.json","ideas.json"],         check=True)
        diff = subprocess.run(["git","diff","--cached","--quiet"])
        if diff.returncode != 0:
            subprocess.run(["git","commit","-m",
                            f"data: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"], check=True)
            subprocess.run(["git","push"], check=True)
            print("OK Pushed to GitHub")
        else:
            print("INFO No changes to push")
    except subprocess.CalledProcessError as e:
        print(f"ERROR Git error: {e}")
        raise

if __name__ == "__main__":
    main()
