import subprocess

backfill_sp500 = True
backfill_raw = False

if backfill_sp500:
	subprocess.run(["python", "scripts/ingest_backfill_sp500.py"], check=True)
	
if backfill_raw:
	subprocess.run(["python", "scripts/ingest_backfill_raw.py"], check=True)

subprocess.run(["python", "scripts/load_sp500.py"], check=True)
subprocess.run(["python", "scripts/ingest_hourly.py"], check=True)
subprocess.run(["python", "scripts/transform.py"], check=True)
subprocess.run(["python", "scripts/load_stock_metrics.py"], check=True)