from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def get_sp500_tickers():
	url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
	tables = pd.read_html(url)
	sp500_tickers = tables[0]["Symbol"].str.replace(".", "-", regex=False).tolist()
	return sp500_tickers

TICKERS = get_sp500_tickers()
BACKFILL_START_DATE = "2005-01-01"
ANALYSIS_START_DATE = "2020-01-01"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ROLLING_WINDOW = 30  # days for volatility

DAGS_DIR = Path(__file__).parent
PROJECT_ROOT = DAGS_DIR.parent
STOCK_TABLE = "stock_metrics"
