"""
Financial Markets Pipeline Demo - Sample Data Version

Generates sample market data to demonstrate the ETL pipeline
without requiring API keys. Perfect for testing and verification.

Usage:
    python demo_pipeline_sample.py
    
Output:
    data/stocks_sample.csv
    data/crypto_sample.csv
    data/forex_sample.csv
"""

import sys
import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import random

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


def generate_stocks_data(data_dir: Path):
    """Generate sample stock market data"""
    logger.info("=" * 60)
    logger.info("STOCKS DATA GENERATION")
    logger.info("=" * 60)
    
    stocks_data = [
        {
            'symbol': 'AAPL',
            'company_name': 'Apple Inc.',
            'price': 178.50 + random.uniform(-5, 5),
            'change': random.uniform(-3, 3),
            'change_percent': random.uniform(-2, 2),
            'volume': random.randint(50000000, 100000000),
            'market_cap': 2800000000000,
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'MSFT',
            'company_name': 'Microsoft Corporation',
            'price': 405.20 + random.uniform(-10, 10),
            'change': random.uniform(-5, 5),
            'change_percent': random.uniform(-1.5, 1.5),
            'volume': random.randint(20000000, 40000000),
            'market_cap': 3000000000000,
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'GOOGL',
            'company_name': 'Alphabet Inc.',
            'price': 142.30 + random.uniform(-8, 8),
            'change': random.uniform(-4, 4),
            'change_percent': random.uniform(-2.5, 2.5),
            'volume': random.randint(15000000, 35000000),
            'market_cap': 1800000000000,
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'AMZN',
            'company_name': 'Amazon.com Inc.',
            'price': 175.80 + random.uniform(-6, 6),
            'change': random.uniform(-3, 3),
            'change_percent': random.uniform(-1.8, 1.8),
            'volume': random.randint(30000000, 60000000),
            'market_cap': 1800000000000,
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'NVDA',
            'company_name': 'NVIDIA Corporation',
            'price': 720.50 + random.uniform(-20, 20),
            'change': random.uniform(-10, 10),
            'change_percent': random.uniform(-3, 3),
            'volume': random.randint(25000000, 50000000),
            'market_cap': 1750000000000,
            'timestamp': datetime.utcnow()
        }
    ]
    
    df = pd.DataFrame(stocks_data)
    output_file = data_dir / 'stocks_sample.csv'
    df.to_csv(output_file, index=False)
    
    logger.info(f"\n{'='*60}")
    logger.info("STOCKS DATA SAMPLE:")
    logger.info(f"{'='*60}")
    print(df.to_string())
    logger.info(f"\n✓ Saved {len(df)} stock records to: {output_file}")
    logger.info(f"{'='*60}\n")


def generate_crypto_data(data_dir: Path):
    """Generate sample cryptocurrency data"""
    logger.info("=" * 60)
    logger.info("CRYPTOCURRENCY DATA GENERATION")
    logger.info("=" * 60)
    
    crypto_data = [
        {
            'symbol': 'BTC',
            'name': 'Bitcoin',
            'price_usd': 51250.00 + random.uniform(-1000, 1000),
            'market_cap': 1000000000000,
            'volume_24h': random.randint(20000000000, 40000000000),
            'change_24h': random.uniform(-5, 5),
            'change_percent_24h': random.uniform(-3, 3),
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'ETH',
            'name': 'Ethereum',
            'price_usd': 2980.00 + random.uniform(-100, 100),
            'market_cap': 350000000000,
            'volume_24h': random.randint(10000000000, 20000000000),
            'change_24h': random.uniform(-100, 100),
            'change_percent_24h': random.uniform(-4, 4),
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'ADA',
            'name': 'Cardano',
            'price_usd': 0.58 + random.uniform(-0.05, 0.05),
            'market_cap': 20000000000,
            'volume_24h': random.randint(300000000, 800000000),
            'change_24h': random.uniform(-0.05, 0.05),
            'change_percent_24h': random.uniform(-6, 6),
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'SOL',
            'name': 'Solana',
            'price_usd': 110.50 + random.uniform(-10, 10),
            'market_cap': 48000000000,
            'volume_24h': random.randint(1500000000, 3000000000),
            'change_24h': random.uniform(-8, 8),
            'change_percent_24h': random.uniform(-5, 5),
            'timestamp': datetime.utcnow()
        },
        {
            'symbol': 'BNB',
            'name': 'Binance Coin',
            'price_usd': 315.00 + random.uniform(-15, 15),
            'market_cap': 47000000000,
            'volume_24h': random.randint(500000000, 1500000000),
            'change_24h': random.uniform(-10, 10),
            'change_percent_24h': random.uniform(-3.5, 3.5),
            'timestamp': datetime.utcnow()
        }
    ]
    
    df = pd.DataFrame(crypto_data)
    output_file = data_dir / 'crypto_sample.csv'
    df.to_csv(output_file, index=False)
    
    logger.info(f"\n{'='*60}")
    logger.info("CRYPTO DATA SAMPLE:")
    logger.info(f"{'='*60}")
    print(df.to_string())
    logger.info(f"\n✓ Saved {len(df)} crypto records to: {output_file}")
    logger.info(f"{'='*60}\n")


def generate_forex_data(data_dir: Path):
    """Generate sample forex data"""
    logger.info("=" * 60)
    logger.info("FOREX DATA GENERATION")
    logger.info("=" * 60)
    
    forex_data = [
        {
            'pair': 'EUR/USD',
            'base_currency': 'EUR',
            'quote_currency': 'USD',
            'rate': 1.0850 + random.uniform(-0.01, 0.01),
            'bid': 1.0848 + random.uniform(-0.01, 0.01),
            'ask': 1.0852 + random.uniform(-0.01, 0.01),
            'change': random.uniform(-0.005, 0.005),
            'change_percent': random.uniform(-0.5, 0.5),
            'timestamp': datetime.utcnow()
        },
        {
            'pair': 'GBP/USD',
            'base_currency': 'GBP',
            'quote_currency': 'USD',
            'rate': 1.2670 + random.uniform(-0.01, 0.01),
            'bid': 1.2668 + random.uniform(-0.01, 0.01),
            'ask': 1.2672 + random.uniform(-0.01, 0.01),
            'change': random.uniform(-0.008, 0.008),
            'change_percent': random.uniform(-0.6, 0.6),
            'timestamp': datetime.utcnow()
        },
        {
            'pair': 'USD/JPY',
            'base_currency': 'USD',
            'quote_currency': 'JPY',
            'rate': 149.80 + random.uniform(-1, 1),
            'bid': 149.78 + random.uniform(-1, 1),
            'ask': 149.82 + random.uniform(-1, 1),
            'change': random.uniform(-0.5, 0.5),
            'change_percent': random.uniform(-0.4, 0.4),
            'timestamp': datetime.utcnow()
        },
        {
            'pair': 'USD/CHF',
            'base_currency': 'USD',
            'quote_currency': 'CHF',
            'rate': 0.8745 + random.uniform(-0.005, 0.005),
            'bid': 0.8743 + random.uniform(-0.005, 0.005),
            'ask': 0.8747 + random.uniform(-0.005, 0.005),
            'change': random.uniform(-0.004, 0.004),
            'change_percent': random.uniform(-0.45, 0.45),
            'timestamp': datetime.utcnow()
        },
        {
            'pair': 'AUD/USD',
            'base_currency': 'AUD',
            'quote_currency': 'USD',
            'rate': 0.6520 + random.uniform(-0.008, 0.008),
            'bid': 0.6518 + random.uniform(-0.008, 0.008),
            'ask': 0.6522 + random.uniform(-0.008, 0.008),
            'change': random.uniform(-0.006, 0.006),
            'change_percent': random.uniform(-0.9, 0.9),
            'timestamp': datetime.utcnow()
        }
    ]
    
    df = pd.DataFrame(forex_data)
    output_file = data_dir / 'forex_sample.csv'
    df.to_csv(output_file, index=False)
    
    logger.info(f"\n{'='*60}")
    logger.info("FOREX DATA SAMPLE:")
    logger.info(f"{'='*60}")
    print(df.to_string())
    logger.info(f"\n✓ Saved {len(df)} forex records to: {output_file}")
    logger.info(f"{'='*60}\n")


def main():
    """Generate sample financial markets data"""
    print("\n" + "="*60)
    print("FINANCIAL MARKETS PIPELINE DEMO - SAMPLE DATA")
    print("="*60 + "\n")
    
    # Ensure output directory exists
    data_dir = ensure_data_directory()
    logger.info(f"Output directory: {data_dir}\n")
    
    try:
        # Generate sample data
        generate_stocks_data(data_dir)
        generate_crypto_data(data_dir)
        generate_forex_data(data_dir)
        
        print("\n" + "="*60)
        print("SAMPLE DATA GENERATION COMPLETED!")
        print("="*60)
        print(f"\nOutput files saved in: {data_dir}")
        print("- stocks_sample.csv (5 major tech stocks)")
        print("- crypto_sample.csv (5 top cryptocurrencies)")
        print("- forex_sample.csv (5 major currency pairs)")
        print("\nNote: This is sample data for demonstration.")
        print("Run demo_pipeline.py with API keys for real market data.")
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Generation failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
