
"""
Daily ETF Price Incremental Ingestion
Fetches only new data since the last available date in Parquet storage.
"""

import io
import os
import boto3
from dotenv import load_dotenv
import sys
import logging
import yfinance as yf
import polars as pl
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------
# CONFIG
# -----------------
from config import TICKERS

load_dotenv()
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

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

def get_latest_ingest_date(engine) -> Optional[datetime]:
	"""Get the most recent date available for this ticker in Postgres."""
	query = "SELECT MAX(date) FROM stock_metrics"
	with engine.connect() as conn:
		res = conn.execute(query).fetchone()[0]
		return res

def fetch_incremental_data(tickers: list[str], start: datetime, end: datetime) -> pl.DataFrame:
	logging.info(f"Fetching {tickers} data from {start.date()} to {end.date()} ")

	df_pd = yf.download(tickers, start=start, end=end, auto_adjust=True)
	if df_pd.empty:
		logging.warning(f"No data returned for {tickers}")
		return pl.DataFrame()
	
	df_pd = df_pd.stack(level=1).rename_axis(["Date", "Ticker"]).reset_index()
	df_pd.columns = [str.lower(col) for col in df_pd.columns.values]

	# convert to polars df
	df = pl.from_pandas(df_pd)
	df = df.with_columns(
		ingest_ts=pl.lit(datetime.now(timezone.utc))
	)

	return df

def download_from_s3(bucket_name: str, key: str) -> tuple[str, pl.DataFrame]:
	"""Download a Parquet file from S3 and return DataFrame."""
	try:
		obj = s3_client.get_object(Bucket=bucket_name, Key=key)
		buffer = io.BytesIO(obj["Body"].read())
		df = pl.read_parquet(buffer)
		return key, df
	except s3_client.exceptions.NoSuchKey:
		return key, pl.DataFrame()

def upload_to_s3(bucket_name: str, key: str, df: pl.DataFrame):
	"""Upload a DataFrame as Parquet to S3."""
	buffer = io.BytesIO()
	df.write_parquet(buffer)
	buffer.seek(0)
	s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
	return key

def save_partitioned_parquet(df: pl.DataFrame, tickers: list[str]):
	"""
	Save DataFrame to Parquet partitioned by year and ticker.
	Load into S3.
	"""
	if df.is_empty(): 
		logging.warning("DataFrame is empty. Nothing to upload.")
		return

	bucket_name = "stock-market-etl"
	years = list(df.select(pl.col("date").dt.year()).unique().to_series())

	# build list of all keys to read
	keys_to_read = [
		f"raw/{year}/{ticker}_metrics.parquet"
		for year in years
		for ticker in tickers
	]

	# parallel read from S3
	existing_data = {}
	with ThreadPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(download_from_s3, bucket_name, key): key for key in keys_to_read}
		for future in as_completed(futures):
			key, existing_df = future.result()
			existing_data[key] = existing_df

	# merge with new data
	merged_data = {}
	for year in years:
		for ticker in tickers:
			key = f"raw/{year}/{ticker}_metrics.parquet"
			subset_df = df.filter(
				(pl.col("date").dt.year() == year) & (pl.col("ticker") == ticker)
			)

			combined_df = pl.concat([existing_data[key], subset_df], how="vertical").unique(
				subset=["date", "ticker"]
			)
			merged_data[key] = combined_df

	# parallel write back to S3
	with ThreadPoolExecutor(max_workers=10) as executor:
		futures = {executor.submit(upload_to_s3, bucket_name, key, df): key for key, df in merged_data.items()}
		for future in as_completed(futures):
			uploaded_key = future.result()

	logging.info("All partitions uploaded successfully.")
	
def main():
	postgres_url=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
	engine = create_engine(postgres_url)

	today = datetime.today()
	last_date = get_latest_ingest_date(engine)
	if not last_date:
		logging.warning(f"No historical data found for {TICKERS}. Run ingest_backfill.py first.")
		return

	start_date = last_date + timedelta(days=1)

	# skip daily ingestion if already up-to-date
	if start_date.date() > today.date():
		logging.info(f"{TICKERS} is already up to date.")
		return
	
	df_new = fetch_incremental_data(TICKERS, start_date, today + timedelta(days=1))
	save_partitioned_parquet(df_new, TICKERS)

	logging.info("Daily incremental ingestion complete.")

if __name__ == "__main__":
	main()

