import os
import io
import sys
import boto3
from dotenv import load_dotenv
import polars as pl
import logging
from sqlalchemy import create_engine
from datetime import datetime
from transform import get_years

# -----------------
# CONFIG
# -----------------
from config import TICKERS, ANALYSIS_START_DATE, STOCK_TABLE

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
def load_to_stock_metrics(table_name: str, year: str, ticker: str, engine):
	"""Load stock metrics from parquet file into Postgres table."""
	bucket_name = "stock-market-etl"
	s3_key = f"enriched/{year}/{ticker}_metrics.parquet"
	try:
		body = s3_resource.Object(bucket_name, s3_key).get()['Body'].read()
	except s3_client.exceptions.NoSuchKey:
		return
	
	buffer = io.BytesIO(body)
	df = pl.read_parquet(buffer)

	if df.is_empty():
		logging.warning("No data to load into Postgres.")
		return
	
	# get latest date in DB per ticker
	query = f"""
		SELECT ticker, MAX(date) AS latest_date
		FROM {table_name}
		GROUP BY ticker
	"""
	with engine.connect() as conn:
		existing_max = pl.DataFrame(conn.execute(query).fetchall())

	# filter out duplicates
	if existing_max.height > 0:
		df = df.join(existing_max, on="ticker", how="left")
		df = df.filter(
			(pl.col("latest_date").is_null()) | (pl.col("date") > pl.col("latest_date"))
		)
	
	df = df.select([col for col in df.columns if col not in ["latest_date", "adj close"]])
	if df.is_empty(): 
		return
	df.write_database(table_name, engine, if_table_exists='append')
	logging.info(f"Loaded {len(df)} new rows into {table_name}.")

def load_to_sp500_companies(companies_table, engine):
	"""Load stock metrics from parquet file into Postgres table."""
	bucket_name = "stock-market-etl"
	s3_key = f"info/sp500_companies.parquet"

	body = s3_resource.Object(bucket_name, s3_key).get()['Body'].read()
	buffer = io.BytesIO(body)
	df = pl.read_parquet(buffer)

	if df.is_empty():
		logging.warning("No company data to load into Postgres.")
		return
	
	# get tickers that already exist in DB
	with engine.connect() as conn:
		existing_tickers = pl.DataFrame(conn.execute(
			f"SELECT ticker_symbol FROM {companies_table}"
		).fetchall())

	# drop duplicates that already exist
	if existing_tickers.height > 0:
		df = df.join(existing_tickers, on="ticker_symbol", how="anti")

	if df.is_empty():
		logging.info("All tickers already exist, nothing new to load.")
		return

	df.write_database(companies_table, engine, if_table_exists="append")
	logging.info(f"Loaded {len(df)} new rows into {companies_table}.")

def main():
	postgres_url=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
	engine = create_engine(postgres_url)
	
	analysis_start_year = datetime.strptime(ANALYSIS_START_DATE, "%Y-%m-%d").year
	years = [yr for yr in get_years("stock-market-etl", "enriched/") if yr >= analysis_start_year]
	
	logging.info(f"Start load to Postgres - found enriched data for years: {years}")
	
	for year in sorted(years):
		for ticker in TICKERS:
			load_to_stock_metrics(STOCK_TABLE, year, ticker, engine)
	load_to_sp500_companies("sp500_companies", engine)

	logging.info("Data load into postgres complete!")
	
if __name__ == "__main__":
	main()
