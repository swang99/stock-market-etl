"""
Backfill Ingestion
Fetches all historical data for the ETFs up to today
"""

import io
import boto3
import sys
import logging
import yfinance as yf
import polars as pl
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

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
def fetch_historical_data(tickers: list[str], start: str, end: str) -> pl.DataFrame:
	"""Fetch historical data for a list of tickers."""
	df_pd = yf.download(tickers, start=start, end=end, auto_adjust=True)
	if df_pd.empty:
		logging.warning(f"No data returned for {tickers}")
		return pl.DataFrame()
	
	df_pd = df_pd.stack(level=1).rename_axis(["Date", "Ticker"]).reset_index()
	df_pd.columns = [str.lower(col) for col in df_pd.columns.values]
	
	# convert to polars df
	df = pl.from_pandas(df_pd)
	df = df.with_columns(ingest_ts=pl.lit(datetime.now(timezone.utc)))
	return df

def upload_partition(bucket_name: str, year: int, ticker: str, df: pl.DataFrame):
	"""Upload a single partition to S3."""
	buffer = io.BytesIO()
	df.write_parquet(buffer)
	buffer.seek(0)
	s3_key = f"raw/{year}/{ticker}_metrics.parquet"
	s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
	return s3_key

def save_partitioned_parquet(df: pl.DataFrame, tickers: list[str]):
	"""Partition by year/ticker and upload in parallel."""
	if df.is_empty():
		logging.warning("DataFrame is empty. Nothing to upload.")
		return

	bucket_name = "stock-market-etl"
	years = list(df.select(pl.col("date").dt.year()).unique().to_series())

	tasks = []
	with ThreadPoolExecutor(max_workers=10) as executor:
		for year in years:
			for ticker in tickers:
				subset_df = df.filter(
					(pl.col("date").dt.year() == year) & (pl.col("ticker") == ticker)
				)
				subset_df = subset_df.with_columns(
					pl.col("volume").cast(pl.Int64)
				)
				if not subset_df.is_empty():
					tasks.append(executor.submit(upload_partition, bucket_name, year, ticker, subset_df))
	
def main():
	hist_df = fetch_historical_data(TICKERS, BACKFILL_START_DATE, END_DATE)
	save_partitioned_parquet(hist_df, TICKERS)
	logging.info("Backfill ingestion of raw data complete!")   

if __name__ == "__main__":
	main()
	