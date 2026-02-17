"""
Financial Markets Pipeline Demo

Demonstrates the complete ETL pipeline by fetching real market data
and saving it to CSV files for verification.

Usage:
    python demo_pipeline.py
    
Output:
    data/stocks_output.csv
    data/crypto_output.csv
    data/forex_output.csv
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add scripts to path
sys.path.append(str(Path(__file__).parent / 'scripts'))

from extractors.stocks_extractor import StocksExtractor
from extractors.crypto_extractor import CryptoExtractor
from extractors.forex_extractor import ForexExtractor
from transformers.stocks_transformer import StocksTransformer
from transformers.crypto_transformer import CryptoTransformer
from transformers.forex_transformer import ForexTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_data_directory():
    """Create data directory if it doesn't exist"""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    return data_dir


def run_stocks_pipeline(api_key: str, data_dir: Path):
    """
    Run stocks extraction and transformation pipeline
    
    Args:
        api_key: Alpha Vantage API key
        data_dir: Directory to save output
    """
    logger.info("=" * 60)
    logger.info("STOCKS PIPELINE")
    logger.info("=" * 60)
    
    # Popular tech stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    
    # Extract
    logger.info(f"Extracting data for symbols: {symbols}")
    extractor = StocksExtractor(api_key)
    raw_data = extractor.extract_multiple_stocks(symbols, delay=12)
    
    if not raw_data:
        logger.warning("No stock data extracted")
        return
    
    # Transform
    logger.info("Transforming stock data...")
    transformer = StocksTransformer()
    transformed_data = []
    
    for stock in raw_data:
        transformed = transformer.transform(stock)
        if transformed:
            transformed_data.append(transformed)
    
    # Convert to DataFrame and save
    if transformed_data:
        df = pd.DataFrame(transformed_data)
        output_file = data_dir / 'stocks_output.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("STOCKS DATA SAMPLE:")
        logger.info(f"{'='*60}")
        print(df.to_string())
        logger.info(f"\n✓ Saved {len(df)} records to: {output_file}")
        logger.info(f"{'='*60}\n")
    else:
        logger.warning("No transformed stock data to save")


def run_crypto_pipeline(data_dir: Path):
    """
    Run crypto extraction and transformation pipeline
    
    Args:
        data_dir: Directory to save output
    """
    logger.info("=" * 60)
    logger.info("CRYPTOCURRENCY PIPELINE")
    logger.info("=" * 60)
    
    # Popular cryptocurrencies
    crypto_ids = ['bitcoin', 'ethereum', 'cardano']
    
    # Extract
    logger.info(f"Extracting data for: {crypto_ids}")
    extractor = CryptoExtractor()
    raw_data = extractor.extract_multiple_cryptos(crypto_ids)
    
    if not raw_data:
        logger.warning("No crypto data extracted")
        return
    
    # Transform
    logger.info("Transforming crypto data...")
    transformer = CryptoTransformer()
    transformed_data = []
    
    for crypto in raw_data:
        transformed = transformer.transform(crypto)
        if transformed:
            transformed_data.append(transformed)
    
    # Convert to DataFrame and save
    if transformed_data:
        df = pd.DataFrame(transformed_data)
        output_file = data_dir / 'crypto_output.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("CRYPTO DATA SAMPLE:")
        logger.info(f"{'='*60}")
        print(df.to_string())
        logger.info(f"\n✓ Saved {len(df)} records to: {output_file}")
        logger.info(f"{'='*60}\n")
    else:
        logger.warning("No transformed crypto data to save")


def run_forex_pipeline(api_key: str, data_dir: Path):
    """
    Run forex extraction and transformation pipeline
    
    Args:
        api_key: Exchange Rate API key
        data_dir: Directory to save output
    """
    logger.info("=" * 60)
    logger.info("FOREX PIPELINE")
    logger.info("=" * 60)
    
    # Major currency pairs
    pairs = [
        ('EUR', 'USD'),
        ('GBP', 'USD'),
        ('USD', 'JPY')
    ]
    
    # Extract
    logger.info(f"Extracting forex data for pairs: {pairs}")
    extractor = ForexExtractor(api_key)
    raw_data = extractor.extract_multiple_pairs(pairs)
    
    if not raw_data:
        logger.warning("No forex data extracted")
        return
    
    # Transform
    logger.info("Transforming forex data...")
    transformer = ForexTransformer()
    transformed_data = []
    
    for forex in raw_data:
        transformed = transformer.transform(forex)
        if transformed:
            transformed_data.append(transformed)
    
    # Convert to DataFrame and save
    if transformed_data:
        df = pd.DataFrame(transformed_data)
        output_file = data_dir / 'forex_output.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"\n{'='*60}")
        logger.info("FOREX DATA SAMPLE:")
        logger.info(f"{'='*60}")
        print(df.to_string())
        logger.info(f"\n✓ Saved {len(df)} records to: {output_file}")
        logger.info(f"{'='*60}\n")
    else:
        logger.warning("No transformed forex data to save")


def main():
    """Run the complete financial markets pipeline demo"""
    print("\n" + "="*60)
    print("FINANCIAL MARKETS PIPELINE DEMO")
    print("="*60 + "\n")
    
    # Get API keys from environment
    alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY', 'demo')
    exchange_rate_key = os.getenv('EXCHANGE_RATE_API_KEY', 'demo')
    
    if alpha_vantage_key == 'demo':
        logger.warning("Using demo API key - rate limits apply")
        logger.info("Set ALPHA_VANTAGE_API_KEY env variable for full access")
    
    # Ensure output directory exists
    data_dir = ensure_data_directory()
    logger.info(f"Output directory: {data_dir}\n")
    
    try:
        # Run pipelines
        run_stocks_pipeline(alpha_vantage_key, data_dir)
        run_crypto_pipeline(data_dir)
        run_forex_pipeline(exchange_rate_key, data_dir)
        
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"\nOutput files saved in: {data_dir}")
        print("- stocks_output.csv")
        print("- crypto_output.csv")
        print("- forex_output.csv")
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
