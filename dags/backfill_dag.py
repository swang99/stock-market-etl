from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pathlib import Path

DAGS_DIR = Path(__file__).parent
SCRIPTS_DIR = DAGS_DIR.parent / "scripts"
backfill_raw_path = str(SCRIPTS_DIR / "ingest_backfill_raw.py")
backfill_sp500_path = str(SCRIPTS_DIR / "ingest_backfill_sp500.py")

with DAG(
	dag_id="stocks_backfill",
	start_date=datetime(2020, 1, 1),
	schedule_interval=None,  # Only run manually
	catchup=False,
	tags=["finance", "etfs", "stocks"]
) as dag:
	
	backfill_raw = BashOperator(
		task_id="ingest_backfill_raw",
		bash_command=f"python '{backfill_raw_path}'"
	)
	
	backfill_sp500 = BashOperator(
		task_id="ingest_backfill_sp500",
		bash_command=f"python '{backfill_sp500_path}'"
	)

	backfill_raw >> backfill_sp500