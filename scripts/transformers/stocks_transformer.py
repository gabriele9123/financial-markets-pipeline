"""
Stock Data Transformer
Cleans and structures stock market data
"""
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict

logger = logging.getLogger(__name__)


class StocksTransformer:
    """Transform stock market data"""
    
    @staticmethod
    def transform(stocks_data: List[Dict]) -> pd.DataFrame:
        """
        Transform raw stock data to structured format
        
        Args:
            stocks_data: List of stock data from API
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Transforming stock data")
        
        all_stocks = []
        
        for stock in stocks_data:
            symbol = stock['symbol']
            quote = stock['data']
            
            try:
                all_stocks.append({
                    'symbol': symbol,
                    'price': float(quote.get('05. price', 0)),
                    'open': float(quote.get('02. open', 0)),
                    'high': float(quote.get('03. high', 0)),
                    'low': float(quote.get('04. low', 0)),
                    'volume': int(quote.get('06. volume', 0)),
                    'latest_trading_day': quote.get('07. latest trading day'),
                    'previous_close': float(quote.get('08. previous close', 0)),
                    'change': float(quote.get('09. change', 0)),
                    'change_percent': quote.get('10. change percent', '').replace('%', ''),
                    'timestamp': datetime.now(),
                    'extracted_at': datetime.now()
                })
            except (ValueError, KeyError) as e:
                logger.error(f"Error transforming data for {symbol}: {e}")
                continue
        
        df = pd.DataFrame(all_stocks)
        
        if df.empty:
            logger.warning("No stock data to transform")
            return df
        
        # Data quality checks
        df = df.dropna(subset=['symbol', 'price'])
        df['price'] = df['price'].astype(float)
        df['volume'] = df['volume'].fillna(0).astype(int)
        
        # Convert change_percent to float
        df['change_percent'] = pd.to_numeric(df['change_percent'], errors='coerce').fillna(0)
        
        logger.info(f"Transformed {len(df)} stock records")
        
        return df
