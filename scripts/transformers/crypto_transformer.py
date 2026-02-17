"""
Crypto Data Transformer
Cleans and structures cryptocurrency data
"""
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class CryptoTransformer:
    """Transform cryptocurrency data"""
    
    @staticmethod
    def transform(crypto_data: Optional[List[Dict]]) -> pd.DataFrame:
        """
        Transform raw crypto data to structured format
        
        Args:
            crypto_data: List of crypto data from CoinGecko API
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Transforming crypto data")
        
        if not crypto_data:
            logger.warning("No crypto data to transform")
            return pd.DataFrame()
        
        all_crypto = []
        
        for crypto in crypto_data:
            try:
                # Handle both simple and detailed API responses
                if 'market_data' in crypto:
                    # Detailed response
                    all_crypto.append({
                        'symbol': crypto.get('symbol', '').upper(),
                        'name': crypto.get('name'),
                        'current_price': float(crypto['market_data']['current_price'].get('usd', 0)),
                        'market_cap': float(crypto['market_data']['market_cap'].get('usd', 0)),
                        'total_volume': float(crypto['market_data']['total_volume'].get('usd', 0)),
                        'price_change_24h': float(crypto['market_data'].get('price_change_percentage_24h', 0)),
                        'timestamp': datetime.now(),
                        'extracted_at': datetime.now()
                    })
                else:
                    # Simple response or markets endpoint
                    coin_id = crypto.get('id', '')
                    data = crypto.get('data', crypto)
                    
                    all_crypto.append({
                        'symbol': crypto.get('symbol', coin_id).upper(),
                        'name': crypto.get('name', coin_id.capitalize()),
                        'current_price': float(data.get('usd', data.get('current_price', 0))),
                        'market_cap': float(data.get('usd_market_cap', data.get('market_cap', 0))),
                        'total_volume': float(data.get('usd_24h_vol', data.get('total_volume', 0))),
                        'price_change_24h': float(data.get('usd_24h_change', data.get('price_change_percentage_24h', 0))),
                        'timestamp': datetime.now(),
                        'extracted_at': datetime.now()
                    })
            except (ValueError, KeyError, TypeError) as e:
                logger.error(f"Error transforming crypto data: {e}")
                continue
        
        df = pd.DataFrame(all_crypto)
        
        if df.empty:
            logger.warning("No crypto data after transformation")
            return df
        
        # Data quality checks
        df = df.dropna(subset=['symbol', 'current_price'])
        df['current_price'] = df['current_price'].astype(float)
        df['market_cap'] = df['market_cap'].fillna(0).astype(float)
        df['price_change_24h'] = df['price_change_24h'].fillna(0).astype(float)
        
        logger.info(f"Transformed {len(df)} crypto records")
        
        return df
