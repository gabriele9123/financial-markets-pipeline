"""
Forex Data Extractor
Fetches exchange rates from ExchangeRate API
"""
import logging
from typing import List, Dict, Optional
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class ForexExtractor(BaseExtractor):
    """Extract forex exchange rates from ExchangeRate API"""
    
    def __init__(self):
        super().__init__(base_url="https://api.exchangerate-api.com/v4/latest")
        
    def extract_exchange_rates(self, base_currency: str, target_currencies: List[str]) -> Optional[Dict]:
        """
        Extract exchange rates for a base currency
        
        Args:
            base_currency: Base currency code (e.g., 'USD')
            target_currencies: List of target currency codes
            
        Returns:
            Exchange rates dictionary
        """
        logger.info(f"Extracting exchange rates for {base_currency} to {', '.join(target_currencies)}")
        
        data = self.get(endpoint=base_currency)
        
        if data and 'rates' in data:
            # Filter for requested currencies
            filtered_rates = {
                'base': base_currency,
                'date': data.get('date'),
                'rates': {
                    currency: data['rates'].get(currency)
                    for currency in target_currencies
                    if currency in data['rates']
                }
            }
            
            logger.info(f"Successfully extracted rates for {len(filtered_rates['rates'])} currencies")
            return filtered_rates
        else:
            logger.error(f"Failed to extract exchange rates for {base_currency}")
            return None
    
    def extract_all_rates(self, base_currency: str) -> Optional[Dict]:
        """
        Extract all available exchange rates for a base currency
        
        Args:
            base_currency: Base currency code
            
        Returns:
            Complete exchange rates data
        """
        logger.info(f"Extracting all exchange rates for {base_currency}")
        
        data = self.get(endpoint=base_currency)
        
        if data:
            logger.info(f"Successfully extracted rates for {len(data.get('rates', {}))} currencies")
            return data
        else:
            logger.error(f"Failed to extract exchange rates for {base_currency}")
            return None
