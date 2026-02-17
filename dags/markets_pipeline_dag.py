"""
Financial Markets Pipeline DAG
Orchestrates ETL for stocks, crypto, and forex data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import yaml
import logging

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extractors.stocks_extractor import StocksExtractor
from extractors.crypto_extractor import CryptoExtractor
from extractors.forex_extractor import ForexExtractor
from transformers.stocks_transformer import StocksTransformer
from transformers.crypto_transformer import CryptoTransformer
from transformers.forex_transformer import ForexTransformer
from loaders.data_loader import DataLoader

# Load configuration
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Get API key from environment
ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'demo')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'gabriele_pascaretta',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': config['pipeline']['max_retries'],
    'retry_delay': timedelta(seconds=config['pipeline']['retry_delay']),
}

# Create DAG
dag = DAG(
    'financial_markets_pipeline',
    default_args=default_args,
    description='ETL pipeline for financial market data',
    schedule_interval=config['pipeline']['schedule_interval'],
    catchup=config['pipeline']['catchup'],
    tags=['finance', 'stocks', 'crypto', 'forex'],
)


def extract_stocks(**context):
    """Extract stock market data"""
    logger.info("Starting stock data extraction")
    
    extractor = StocksExtractor(ALPHA_VANTAGE_KEY)
    symbols = config['data_sources']['stocks']['symbols']
    
    data = extractor.extract_multiple_stocks(symbols)
    
    context['ti'].xcom_push(key='stocks_raw_data', value=data)
    logger.info(f"Extracted data for {len(data)} stocks")


def extract_crypto(**context):
    """Extract cryptocurrency data"""
    logger.info("Starting crypto data extraction")
    
    extractor = CryptoExtractor()
    coin_ids = [coin['id'] for coin in config['data_sources']['crypto']['coins']]
    
    data = extractor.extract_crypto_details(coin_ids)
    
    context['ti'].xcom_push(key='crypto_raw_data', value=data)
    
    logger.info(f"Extracted data for {len(data) if data else 0} cryptocurrencies")


def extract_forex(**context):
    """Extract forex exchange rates"""
    logger.info("Starting forex data extraction")
    
    extractor = ForexExtractor()
    base_currency = config['data_sources']['forex']['base_currency']
    target_currencies = config['data_sources']['forex']['target_currencies']
    
    data = extractor.extract_exchange_rates(base_currency, target_currencies)
    
    context['ti'].xcom_push(key='forex_raw_data', value=data)
    logger.info(f"Extracted forex rates")


def transform_stocks(**context):
    """Transform stock data"""
    logger.info("Starting stock data transformation")
    
    raw_data = context['ti'].xcom_pull(key='stocks_raw_data', task_ids='extract_stocks')
    
    if not raw_data:
        logger.warning("No stock data to transform")
        return
    
    transformer = StocksTransformer()
    df = transformer.transform(raw_data)
    
    context['ti'].xcom_push(key='stocks_transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} stock records")


def transform_crypto(**context):
    """Transform crypto data"""
    logger.info("Starting crypto data transformation")
    
    raw_data = context['ti'].xcom_pull(key='crypto_raw_data', task_ids='extract_crypto')
    
    if not raw_data:
        logger.warning("No crypto data to transform")
        return
    
    transformer = CryptoTransformer()
    df = transformer.transform(raw_data)
    
    context['ti'].xcom_push(key='crypto_transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} crypto records")


def transform_forex(**context):
    """Transform forex data"""
    logger.info("Starting forex data transformation")
    
    raw_data = context['ti'].xcom_pull(key='forex_raw_data', task_ids='extract_forex')
    
    if not raw_data:
        logger.warning("No forex data to transform")
        return
    
    transformer = ForexTransformer()
    df = transformer.transform(raw_data)
    
    context['ti'].xcom_push(key='forex_transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} forex records")


def load_stocks(**context):
    """Load stock data to database"""
    logger.info("Starting stock data loading")
    
    import pandas as pd
    
    data = context['ti'].xcom_pull(key='stocks_transformed_data', task_ids='transform_stocks')
    
    if not data:
        logger.warning("No stock data to load")
        return
    
    df = pd.DataFrame(data)
    
    db_path = config['database']['path']
    loader = DataLoader(db_path)
    
    count = loader.load_stocks(df)
    logger.info(f"Loaded {count} stock records to database")


def load_crypto(**context):
    """Load crypto data to database"""
    logger.info("Starting crypto data loading")
    
    import pandas as pd
    
    data = context['ti'].xcom_pull(key='crypto_transformed_data', task_ids='transform_crypto')
    
    if not data:
        logger.warning("No crypto data to load")
        return
    
    df = pd.DataFrame(data)
    
    db_path = config['database']['path']
    loader = DataLoader(db_path)
    
    count = loader.load_crypto(df)
    logger.info(f"Loaded {count} crypto records to database")


def load_forex(**context):
    """Load forex data to database"""
    logger.info("Starting forex data loading")
    
    import pandas as pd
    
    data = context['ti'].xcom_pull(key='forex_transformed_data', task_ids='transform_forex')
    
    if not data:
        logger.warning("No forex data to load")
        return
    
    df = pd.DataFrame(data)
    
    db_path = config['database']['path']
    loader = DataLoader(db_path)
    
    count = loader.load_forex(df)
    logger.info(f"Loaded {count} forex records to database")


# Define tasks
extract_stocks_task = PythonOperator(
    task_id='extract_stocks',
    python_callable=extract_stocks,
    dag=dag,
)

extract_crypto_task = PythonOperator(
    task_id='extract_crypto',
    python_callable=extract_crypto,
    dag=dag,
)

extract_forex_task = PythonOperator(
    task_id='extract_forex',
    python_callable=extract_forex,
    dag=dag,
)

transform_stocks_task = PythonOperator(
    task_id='transform_stocks',
    python_callable=transform_stocks,
    dag=dag,
)

transform_crypto_task = PythonOperator(
    task_id='transform_crypto',
    python_callable=transform_crypto,
    dag=dag,
)

transform_forex_task = PythonOperator(
    task_id='transform_forex',
    python_callable=transform_forex,
    dag=dag,
)

load_stocks_task = PythonOperator(
    task_id='load_stocks',
    python_callable=load_stocks,
    dag=dag,
)

load_crypto_task = PythonOperator(
    task_id='load_crypto',
    python_callable=load_crypto,
    dag=dag,
)

load_forex_task = PythonOperator(
    task_id='load_forex',
    python_callable=load_forex,
    dag=dag,
)

# Set task dependencies - parallel extraction and processing
extract_stocks_task >> transform_stocks_task >> load_stocks_task
extract_crypto_task >> transform_crypto_task >> load_crypto_task
extract_forex_task >> transform_forex_task >> load_forex_task
