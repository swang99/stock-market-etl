import io
import os
import boto3
import sys
import logging
import polars as pl
import pandas as pd
import urllib

# -----------------
# CONFIG
# -----------------
from config import TICKERS, BACKFILL_START_DATE, END_DATE
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# -----------------
# LOGGING
# -----------------
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s | %(levelname)s | %(message)s",
	handlers=[logging.StreamHandler(sys.stdout)],
)

# -----------------
# FUNCTIONS
# -----------------
def save_sp_500():
	# scrape table
	url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
	req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
	tables = pd.read_html(urllib.request.urlopen(req), flavor="bs4")
	sp500_df = tables[0]
	sp500_df["Symbol"] = sp500_df["Symbol"].str.replace(".", "-", regex=False)
	sp500_df = sp500_df.drop(columns=["Date added", "CIK", "Founded"])
	sp500_df.columns = ["ticker_symbol", "security_name", "gics_sector", "gics_sub_industry", "headquarters"]
	sp500_df = pl.from_pandas(sp500_df)

	# write data without local download
	buffer = io.BytesIO()
	sp500_df.write_parquet(buffer)
	buffer.seek(0)

	# save locally for Streamlit cloud
	local_dir = "data"
	os.makedirs(local_dir, exist_ok=True)
	local_csv_path = os.path.join(local_dir, "sp500_companies.csv")
	sp500_df.write_csv(local_csv_path)
	logging.info(f"Saved local S&P 500 data → {local_csv_path}")
	
	# save to S3
	bucket_name = "stock-market-etl"
	s3_key = f"info/sp500_companies.parquet"
	s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

	logging.info(f"Loaded S&P 500 company info into S3")
	
def main():
	save_sp_500()
	logging.info("Backfill ingestion of S&P 500 metadata complete!")   

if __name__ == "__main__":
	main()
