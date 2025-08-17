from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from pathlib import Path

DAGS_DIR = Path(__file__).parent
PROJECT_ROOT = DAGS_DIR.parent
SCRIPTS_DIR = DAGS_DIR.parent / "scripts"

ingest_path = str(SCRIPTS_DIR / "ingest_hourly.py")
transform_path = str(SCRIPTS_DIR / "transform.py")
load_path = str(SCRIPTS_DIR / "load_to_db.py")

default_args = {
	"owner": "you",
	"depends_on_past": False,
	"email_on_failure": False,
	"email_on_retry": False,
	"retries": 1,
	"retry_delay": timedelta(minutes=5),
}

with DAG(
	dag_id="stocks_hourly",
	default_args=default_args,
	description="Hourly stocks analytics pipeline",
	schedule="0 9-17 * * 1-5",
	start_date=datetime(2025, 8, 15),
	catchup=False,
	tags=["finance", "etfs", "stocks"]
) as dag:

	ingest_daily = BashOperator(
		task_id="ingest_hourly",
		bash_command=f"python '{ingest_path}'"
	)

	transform = BashOperator(
		task_id="transform_data",
		bash_command=f"python '{transform_path}'"
	)

	load = BashOperator(
		task_id="load_data",
		bash_command=f"python '{load_path}'"
	)

	ingest_daily >> transform >> load