# Financial Markets Data Pipeline

Production-ready ETL pipeline for real-time financial market data using Apache Airflow. Tracks stocks, cryptocurrencies, and forex rates with time-series analysis capabilities.

## 🎯 Project Overview

This project demonstrates:
- **Real-Time Data Processing**: Live market data ingestion
- **Time-Series Analysis**: Historical price tracking and trends
- **Financial Data Modeling**: OHLC (Open, High, Low, Close) data structures
- **Multi-Source Integration**: Stocks, crypto, and forex from different APIs
- **Apache Airflow Orchestration**: Scheduled data collection
- **Data Quality**: Validation and anomaly detection

## 🏗️ Architecture

```
┌────────────-─┐  ┌─────────────┐  ┌─────────────┐
│ Alpha Vantage│  │  CoinGecko  │  │ ExchangeRate│
│  (Stocks)    │  │  (Crypto)   │  │   (Forex)   │
└──────┬─────-─┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │ Extract (Parallel)
                        ▼
               ┌─────────────────┐
               │   Transform     │
               │  - Normalize    │
               │  - Calculate    │
               │  - Validate     │
               └────────┬────────┘
                        │ Load
                        ▼
               ┌─────────────────┐
               │    Database     │
               │  Time-Series DB │
               └─────────────────┘
```

## 📊 Data Sources

1. **Alpha Vantage API** - Stock market data (free tier: 25 requests/day)
2. **CoinGecko API** - Cryptocurrency prices (completely free, no key needed)
3. **ExchangeRate API** - Forex rates (free tier: 1500 requests/month)

## 🛠️ Tech Stack

- Python 3.9+
- Apache Airflow 2.10+
- SQLite (with time-series optimizations)
- Pandas (Financial data analysis)
- SQLAlchemy (Database ORM)
- Requests (API integration)

## 📁 Project Structure

```
financial-markets-pipeline/
├── dags/
│   └── markets_pipeline_dag.py      # Airflow DAG
├── scripts/
│   ├── extractors/
│   │   ├── base_extractor.py        # Base API class
│   │   ├── stocks_extractor.py      # Stock data extraction
│   │   ├── crypto_extractor.py      # Crypto data extraction
│   │   └── forex_extractor.py       # Forex data extraction
│   ├── transformers/
│   │   ├── stocks_transformer.py    # Stock data transformation
│   │   ├── crypto_transformer.py    # Crypto data transformation
│   │   └── forex_transformer.py     # Forex data transformation
│   └── loaders/
│       ├── database_schema.py       # Database models
│       └── data_loader.py           # Data loading logic
├── config/
│   └── config.yaml                  # Pipeline configuration
├── data/
│   └── markets.db                   # SQLite database
├── tests/
│   └── test_pipeline.py             # Unit tests
├── requirements.txt                 # Dependencies
└── README.md                        # This file
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Alpha Vantage API key (free at https://www.alphavantage.co/support/#api-key)

### Installation

1. **Navigate to project**
   ```bash
   cd financial-markets-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure API keys**
   ```bash
   cp .env.example .env
   # Edit .env and add your Alpha Vantage API key
   ```

### Running the Pipeline

#### With Airflow

```bash
export AIRFLOW_HOME=$(pwd)
airflow db init

airflow users create \
    --username admin \
    --password admin \
    --firstname Gabriele \
    --lastname Pascaretta \
    --role Admin \
    --email gabriele.pascaretta@gmail.com

# Start services
airflow webserver --port 8080  # Terminal 1
airflow scheduler              # Terminal 2
```

Access at `http://localhost:8080`

#### Test Individual Components

```bash
# Test stock extraction
python -c "from scripts.extractors.stocks_extractor import StocksExtractor; e = StocksExtractor('demo'); print(e.extract_stock_price('IBM'))"

# Test crypto extraction
python -c "from scripts.extractors.crypto_extractor import CryptoExtractor; e = CryptoExtractor(); print(e.extract_crypto_prices(['bitcoin', 'ethereum']))"

# Run tests
pytest tests/ -v
```

## 📈 Usage

1. Enable the `financial_markets_pipeline` DAG in Airflow UI
2. Pipeline runs every 4 hours to collect market data
3. Data is stored in SQLite with time-series indexing
4. Query the database for analysis

## 🔍 Data Schema

### Stocks Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| symbol | TEXT | Stock ticker (AAPL, GOOGL) |
| price | REAL | Current price |
| open | REAL | Opening price |
| high | REAL | Daily high |
| low | REAL | Daily low |
| volume | INTEGER | Trading volume |
| timestamp | DATETIME | Market timestamp |
| extracted_at | DATETIME | Collection time |

### Crypto Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| symbol | TEXT | Crypto symbol (BTC, ETH) |
| name | TEXT | Full name |
| current_price | REAL | Current USD price |
| market_cap | REAL | Market capitalization |
| total_volume | REAL | 24h trading volume |
| price_change_24h | REAL | 24h price change % |
| timestamp | DATETIME | Data timestamp |
| extracted_at | DATETIME | Collection time |

### Forex Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| base_currency | TEXT | Base currency (USD) |
| target_currency | TEXT | Target currency (EUR) |
| exchange_rate | REAL | Exchange rate |
| timestamp | DATETIME | Rate timestamp |
| extracted_at | DATETIME | Collection time |

## 📊 Example Queries

```sql
-- Latest stock prices
SELECT symbol, price, volume, timestamp
FROM stocks
WHERE timestamp = (SELECT MAX(timestamp) FROM stocks)
ORDER BY volume DESC;

-- Crypto price changes (24h)
SELECT name, current_price, price_change_24h
FROM crypto
WHERE timestamp = (SELECT MAX(timestamp) FROM crypto)
ORDER BY price_change_24h DESC
LIMIT 10;

-- Forex rates trend (EUR/USD)
SELECT exchange_rate, timestamp
FROM forex
WHERE base_currency = 'USD' AND target_currency = 'EUR'
ORDER BY timestamp DESC
LIMIT 30;

-- Stock price volatility (last 7 days)
SELECT 
    symbol,
    AVG(high - low) as avg_daily_range,
    MAX(high) as week_high,
    MIN(low) as week_low
FROM stocks
WHERE timestamp >= datetime('now', '-7 days')
GROUP BY symbol
ORDER BY avg_daily_range DESC;
```

## 🧪 Testing

```bash
pytest tests/ -v
pytest --cov=scripts tests/
```

## 📝 Key Skills Demonstrated

- **Financial Data Engineering**: Real-time market data processing
- **Time-Series Analysis**: Historical price tracking and trends
- **API Integration**: Multiple financial data sources
- **Data Validation**: Price anomaly detection
- **Database Optimization**: Indexing for time-series queries
- **Production Practices**: Error handling, logging, testing

## 🚧 Future Enhancements

- Add technical indicators (RSI, MACD, Moving Averages)
- Implement price alerts and notifications
- Add portfolio tracking features
- Integrate with data visualization (Plotly/Streamlit)
- Add machine learning price predictions
- Implement real-time WebSocket connections

## 👤 Author

**Gabriele Pascaretta**
- LinkedIn: [gabriele-pascaretta](https://www.linkedin.com/in/gabriele-pascaretta/)
- GitHub: [@gabriele9123](https://github.com/gabriele9123)
- Email: gabriele.pascaretta@gmail.com

---

*Built to demonstrate financial data engineering skills and real-time data processing capabilities.*
