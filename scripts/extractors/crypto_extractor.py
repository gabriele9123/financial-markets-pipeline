"""
Cryptocurrency Data Extractor
Fetches crypto prices from CoinGecko API
"""
import logging
from typing import List, Dict, Optional
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class CryptoExtractor(BaseExtractor):
    """Extract cryptocurrency data from CoinGecko API"""
    
    def __init__(self):
        super().__init__(base_url="https://api.coingecko.com/api/v3")
        
    def extract_crypto_prices(self, coin_ids: List[str]) -> Optional[List[Dict]]:
        """
        Extract current prices for cryptocurrency coins
        
        Args:
            coin_ids: List of CoinGecko coin IDs (e.g., ['bitcoin', 'ethereum'])
            
        Returns:
            List of crypto data
        """
        logger.info(f"Extracting crypto data for: {', '.join(coin_ids)}")
        
        params = {
            'ids': ','.join(coin_ids),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        data = self.get(endpoint="simple/price", params=params)
        
        if data:
            # Transform into list format
            results = []
            for coin_id, coin_data in data.items():
                results.append({
                    'id': coin_id,
                    'data': coin_data
                })
            
            logger.info(f"Successfully extracted data for {len(results)} cryptocurrencies")
            return results
        else:
            logger.error("Failed to extract crypto data")
            return None
    
    def extract_crypto_details(self, coin_ids: List[str]) -> Optional[List[Dict]]:
        """
        Extract detailed information for cryptocurrencies
        
        Args:
            coin_ids: List of coin IDs
            
        Returns:
            List of detailed crypto data
        """
        logger.info(f"Extracting detailed crypto data for {len(coin_ids)} coins")
        
        params = {
            'ids': ','.join(coin_ids),
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': len(coin_ids),
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        }
        
        data = self.get(endpoint="coins/markets", params=params)
        
        if data and isinstance(data, list):
            logger.info(f"Successfully extracted detailed data for {len(data)} cryptocurrencies")
            return data
        else:
            logger.error("Failed to extract detailed crypto data")
            return None
