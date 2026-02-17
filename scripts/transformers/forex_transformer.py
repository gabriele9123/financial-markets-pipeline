"""
Forex Data Transformer
Cleans and structures foreign exchange data
"""
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ForexTransformer:
    """Transform forex exchange rate data"""
    
    @staticmethod
    def transform(forex_data: Optional[Dict]) -> pd.DataFrame:
        """
        Transform raw forex data to structured format
        
        Args:
            forex_data: Forex rates data from API
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Transforming forex data")
        
        if not forex_data or 'rates' not in forex_data:
            logger.warning("No forex data to transform")
            return pd.DataFrame()
        
        base_currency = forex_data['base']
        rates = forex_data['rates']
        rate_date = forex_data.get('date')
        
        all_rates = []
        
        for target_currency, exchange_rate in rates.items():
            try:
                all_rates.append({
                    'base_currency': base_currency,
                    'target_currency': target_currency,
                    'exchange_rate': float(exchange_rate),
                    'rate_date': rate_date,
                    'timestamp': datetime.now(),
                    'extracted_at': datetime.now()
                })
            except (ValueError, TypeError) as e:
                logger.error(f"Error transforming rate for {target_currency}: {e}")
                continue
        
        df = pd.DataFrame(all_rates)
        
        if df.empty:
            logger.warning("No forex data after transformation")
            return df
        
        # Data quality checks
        df = df.dropna(subset=['base_currency', 'target_currency', 'exchange_rate'])
        df['exchange_rate'] = df['exchange_rate'].astype(float)
        
        logger.info(f"Transformed {len(df)} forex rate records")
        
        return df
