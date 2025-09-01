import os
import io
import sys
import boto3
from dotenv import load_dotenv
import polars as pl
import logging
from sqlalchemy import create_engine, text

# -----------------
# CONFIG
# -----------------
from config import TICKERS, STOCK_TABLE

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

	df.write_database(companies_table, engine, if_table_exists="replace")
	logging.info(f"Loaded {len(df)} new rows into {companies_table}.")

def main():
	postgres_url=f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
	engine = create_engine(postgres_url)
	
	logging.info(f"Start S&P 500 load to Postgres")
	load_to_sp500_companies("sp500_companies", engine)
	logging.info("S&P 500 load into postgres complete!")
	
if __name__ == "__main__":
	main()
