import logging
import yfinance as yf
import polars as pl
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

df_pd = yf.download("BRK-B", start=datetime(2000, 1, 1), end=datetime(2003, 1, 1), auto_adjust=True)
if df_pd.empty:
	print("Empty data for BRK-B")
print(df_pd.head())