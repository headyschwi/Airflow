import yfinance

def get_history(ticker, start_date, end_date):
    path = f"/home/eder/Desktop/Airflow/Financial/data/raw/{ticker}.csv"

    data = yfinance.Ticker(ticker).history(
        period = "1d",
        interval = "1h",
        start = start_date,
        end = end_date,
        prepost = True
    )

    data.to_csv(path)

if __name__ == "__main__":
    get_history("AAPL", "2024-01-01", "2024-01-31")