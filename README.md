# Portfolio Risk Dashboard

Live risk scores for 21 stocks, auto-updated daily. Includes an ideas tracker for posting charts and notes.

## Setup (15 minutes)

### 1. Create a new GitHub repo
Go to github.com → New repository → name it `portfolio-dashboard` → **Public** → tick "Add README" → Create.

### 2. Upload all files
Upload these 5 files to the repo root:
- `index.html`
- `generate_data.py`
- `ideas.json`
- `README.md`

And create the workflow file at `.github/workflows/update_data.yml` (same as the risk-alerts repo — create via Add File → Create New File, type the path).

### 3. Enable GitHub Pages
Settings → Pages → Branch: **main** → Folder: **/ (root)** → Save

### 4. Run the data generator (first time)
Actions → **Update Dashboard Data** → Run workflow

This will fetch all 21 stocks and create `data.json`. Takes ~2 minutes.

### 5. Visit your dashboard
`https://dannypeters15.github.io/portfolio-dashboard/`

---

## Ideas Tracker Setup

To post ideas, you need a GitHub PAT with repo write access:

1. Go to: github.com/settings/tokens/new
2. Scopes: tick **repo** (full control)
3. Copy the token (starts with `ghp_`)
4. In the dashboard, click ⚙ Settings
5. Enter: Owner = `dannypeters15`, Repo = `portfolio-dashboard`, PAT = your token
6. Click Save

Anyone can **read** ideas without a PAT. Only people with the PAT can **post**.

---

## Adding New Stocks

Edit `generate_data.py` and add to `STOCK_LIST`:

```python
"AAPL": {"name": "Apple", "inception": "1980-12-12", "allocation": 5},
```

Then re-run the GitHub Action (or wait for the daily run).

Also add the ticker to `check_risk.py` in the risk-alerts repo if you want email/push alerts for it.

---

## How Data Updates Work

The GitHub Action runs every weekday at 9:30 PM UTC (after US market close):
1. Fetches full price history from Yahoo Finance for all 21 stocks
2. Calculates log-linear regression risk scores
3. Writes `data.json` to the repo
4. GitHub Pages serves the updated file → dashboard auto-refreshes

---

## Ticker Reference

| Ticker | Company | Exchange |
|--------|---------|----------|
| MSFT | Microsoft | NASDAQ |
| AMZN | Amazon | NASDAQ |
| NVDA | Nvidia | NASDAQ |
| META | Meta Platforms | NASDAQ |
| ORCL | Oracle | NYSE |
| AVGO | Broadcom | NASDAQ |
| NFLX | Netflix | NASDAQ |
| TSLA | Tesla | NASDAQ |
| AMD | AMD | NASDAQ |
| PLTR | Palantir | NYSE |
| SPOT | Spotify | NYSE |
| MSTR | MicroStrategy | NASDAQ |
| TTWO | Take-Two Interactive | NASDAQ |
| TGT | Target | NYSE |
| SONY | Sony Group ADR | NYSE |
| IREN | Iris Energy | NASDAQ |
| MP | MP Materials | NYSE |
| USAR | USA Rare Earth | — |
| URA | Global X Uranium ETF | NYSE Arca |
| IFC.TO | Intact Financial | TSX |
| RR.L | Rolls-Royce | LSE |

Not financial advice.
