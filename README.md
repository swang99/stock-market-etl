# 📈 Stock Market ETL Pipeline

A data pipeline that ingests, transforms, and loads stock market data into a Postgres database.  Currently scheduled to run hourly, with potential support for real-time streaming in the future.

## 🚀 Features
- Ingests stock market data for configurable tickers (via `config.py`).
- ETL workflow orchestrated using **Apache Airflow**.
- Stores cleaned and structured data in **Postgres**.
- Supports historical backfills for reproducible analytics.
- Modular design for extending new data sources or destinations.


## 📂 Project Structure
```yaml
stock-market-etl/
│── dags/ # Airflow DAG definitions
│── scripts/ # Python ETL scripts
│── config/ # Config file for tickers and parameters
│── sql/ # SQL scripts for schema creation
│── tests/ # Unit tests
│── requirements.txt # Python dependencies
│── README.md # Project documentation
```

## ⚙️ Setup

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/stock-market-etl.git
cd stock-market-etl
```

### 2. Create a virtual environment & install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure environment variables
Create a .env file with:
```env
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=stock-etl-postgres.cb6scgosoans.us-east-2.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DB=stocks_etl_db
```

### 4. Run Airflow locally
```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```
Then open http://localhost:8080.

## 📊 Usage

### Backfill data
```bash
airflow dags backfill stock_market_etl \
    --start-date 2020-01-01 --end-date 2020-12-31
```

### ⏰ Scheduling
The DAG is currently scheduled as:

Hourly refresh (9–5, weekdays): 0 9-17 * * 1-5

Streaming: via Kafka or other event-driven tools.

## 📌 Roadmap
 Experiment with streaming ingestion (Kafka, Spark)
 Add data quality checks (Great Expectations, dbt tests)
 Expose API or dashboard for data access