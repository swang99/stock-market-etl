from datetime import datetime, timezone
from pathlib import Path
import requests 
from bs4 import BeautifulSoup
import pytz

def get_sp500_tickers():
	url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
	headers = { 
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
		"AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/115.0 Safari/537.36"
	}

	html_doc = requests.get(url, headers=headers)
	soup = BeautifulSoup(html_doc.text, "html.parser")

	table = soup.find("table", id="constituents")
	sp500_data = []
	
	for row in table.find_all("tr")[1:]:
		cols = row.find_all("td")
		if cols:
			ticker = cols[0].text.strip().replace(".", "-")
			security_name = cols[1].text.strip()
			gics_sector = cols[2].text.strip()
			gics_sub_industry = cols[3].text.strip()
			sp500_data.append([
				ticker, security_name, gics_sector, gics_sub_industry
			])

	return sp500_data

SP500_INFO = get_sp500_tickers()
TICKERS = [t[0] for t in SP500_INFO]

BACKFILL_START_DATE = "2005-01-01"
AN_START_DATE = "2020-01-01"
END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ROLLING_WINDOW = 30  # days for volatility

DAGS_DIR = Path(__file__).parent
PROJECT_ROOT = DAGS_DIR.parent
STOCK_TABLE = "stock_metrics"

TIME_ZONE = pytz.timezone("US/Eastern")
