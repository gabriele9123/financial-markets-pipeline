"""
Stock Market Data Extractor
Fetches stock prices from Alpha Vantage API
"""
import logging
import time
from typing import List, Dict, Optional
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class StocksExtractor(BaseExtractor):
    """Extract stock market data from Alpha Vantage API"""
    
    def __init__(self, api_key: str):
        super().__init__(base_url="https://www.alphavantage.co/query")
        self.api_key = api_key
        
    def extract_stock_price(self, symbol: str) -> Optional[Dict]:
        """
        Extract current price for a stock symbol
        
        Args:
            symbol: Stock ticker (e.g., 'AAPL', 'GOOGL')
            
        Returns:
            Stock data dictionary
        """
        logger.info(f"Extracting stock data for: {symbol}")
        
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol,
            'apikey': self.api_key
        }
        
        data = self.get(params=params)
        
        if data and 'Global Quote' in data:
            quote = data['Global Quote']
            if quote:  # Check if quote is not empty
                logger.info(f"Successfully extracted data for {symbol}")
                return {
                    'symbol': symbol,
                    'data': quote
                }
            else:
                logger.warning(f"Empty quote data for {symbol}")
                return None
        else:
            logger.error(f"Failed to extract data for {symbol}")
            return None
    
    def extract_multiple_stocks(self, symbols: List[str], delay: int = 12) -> List[Dict]:
        """
        Extract data for multiple stock symbols
        
        Args:
            symbols: List of stock tickers
            delay: Delay between API calls (Alpha Vantage rate limit)
            
        Returns:
            List of stock data
        """
        results = []
        
        for i, symbol in enumerate(symbols):
            data = self.extract_stock_price(symbol)
            if data:
                results.append(data)
            
            # Add delay between calls (except for last one)
            if i < len(symbols) - 1:
                logger.info(f"Waiting {delay}s before next request (rate limit)...")
                time.sleep(delay)
                
        logger.info(f"Extracted data for {len(results)}/{len(symbols)} stocks")
        return results
