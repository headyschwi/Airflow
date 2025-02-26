import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
from pathlib import Path
import pendulum

TICKERS = ["APPL", "GOOG", "QDVE.DE", "TSLA", "MSFT"]

@task()
def get_history(ticker, ds=None, ds_nodash=None):

    file_path = f"/home/eder/Desktop/Airflow/Financial/data/raw/{ticker}/{ds}.csv"
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    data = yfinance.Ticker(ticker).history(
        period = "1d",
        interval = "1h",
        start = ds_add(ds, -1),
        end = ds,
        prepost = True
    )

    data.to_csv(file_path)

@dag(
    schedule_interval = "0 0 * * 2-6",
    start_date = pendulum.datetime(2024,1,1, tz="UTC"),
    catchup = True
)
def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id=ticker)(ticker)

dag = get_stocks_dag()