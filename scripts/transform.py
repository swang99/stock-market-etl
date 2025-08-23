import io
import boto3
import sys
import logging
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------
# CONFIG
# -----------------
from config import TICKERS, ROLLING_WINDOW
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
def get_years(bucket_name, prefix) -> list[int]:
	response = s3_client.list_objects_v2(
		Bucket=bucket_name, 
		Prefix=prefix,
		Delimiter="/"
	)

	# Extract years from keys
	years = set()
	for prefix_obj in response.get("CommonPrefixes", []):
		folder = prefix_obj.get("Prefix", "")  # e.g. raw/2024/
		parts = folder.strip("/").split("/")
		if len(parts) > 1 and parts[1].isdigit():
			years.add(int(parts[1]))
	
	return sorted(list(years))


def load_raw_df(year: str, ticker: str) -> pl.DataFrame:
	"""Load raw parquet files from s3"""
	bucket_name = "stock-market-etl"
	s3_key = f"raw/{year}/{ticker}_metrics.parquet"
	
	try:
		s3_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
	except s3_client.exceptions.NoSuchKey:
		return pl.DataFrame()
	
	buffer = io.BytesIO(s3_obj['Body'].read())
	df = pl.read_parquet(buffer)
	df = df.with_columns(pl.col("volume").cast(pl.Int64))
	return df

def compute_metrics(df: pl.DataFrame) -> pl.DataFrame:
	if df.is_empty(): return df

	q = (
		df.lazy()
		.sort(["ticker", "date"]) 
		.with_columns([
			(100 * pl.col("close").pct_change().over("ticker")).alias("daily_return"),
		])
		.with_columns([
			pl.col("daily_return")
			.rolling_std(window_size=ROLLING_WINDOW, min_samples=1)
			.over("ticker")
			.alias("rolling_vol_30d"),
		])
	)

	return q.collect()

def data_quality_checks(df: pl.DataFrame) -> bool:
	# print("data quality check: ", df.dtypes)
	expected_schema = {
		"date": pl.Datetime, "close": pl.Float64, "high": pl.Float64,
		"low": pl.Float64, "open": pl.Float64, "volume": pl.Int64,
		"ticker": pl.Utf8, "ingest_ts": pl.Datetime, "daily_return": pl.Float64,
		"rolling_vol_30d": pl.Float64,
	}

	# Check columns present
	missing_cols = [col for col in expected_schema if col not in df.columns]
	if missing_cols:
		logging.error(f"Missing columns: {missing_cols}")
		return False
	
	# Check column types
	for col, expected_type in expected_schema.items():
		if col in df.columns:
			actual_type = df.schema[col]
			if not isinstance(actual_type, expected_type):
				logging.error(f"Column {col} expected type {expected_type}, found {actual_type}")
				return False

	# Check for nulls in critical columns
	crit_columns = ['ticker', 'date']
	null_counts = df.select(pl.col(c).is_null().sum().alias(c) for c in crit_columns).to_dict(as_series=False)
	for col, count in null_counts.items():
		if count[0] > 0:
			logging.error(f"Null values found in column {col}: {count}")
			return False
	
	return True


def save_enriched_data(df: pl.DataFrame, year: str, ticker: str):
	if df.is_empty():
		logging.warning(f"No data to save for {year}")
		return

	# load enriched data into s3 without local download needed
	buffer = io.BytesIO()
	df.write_parquet(buffer)
	buffer.seek(0)

	bucket_name = "stock-market-etl"
	s3_key = f"enriched/{year}/{ticker}_metrics.parquet"
	s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
		
def process_ticker(year: int, ticker: str):
	raw_df = load_raw_df(year, ticker)
	if raw_df.is_empty():
		return

	enriched_df = compute_metrics(raw_df)
	if data_quality_checks(enriched_df):
		save_enriched_data(enriched_df, year, ticker)
	else:
		logging.warning(f"Data quality failed for {year}/{ticker}")

def main():
	years = get_years("stock-market-etl", "raw/")
	
	for year in years:
		logging.info(f"Processing year {year}")
		with ThreadPoolExecutor(max_workers=10) as executor:
			futures = [executor.submit(process_ticker, year, ticker) for ticker in TICKERS]
			for future in as_completed(futures):
				future.result()  # propagate exceptions

	logging.info(f"Saved all enriched data to S3")

if __name__ == "__main__":
	main()
