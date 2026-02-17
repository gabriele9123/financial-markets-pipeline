"""
Data Loader
Loads transformed financial data into database
"""
import logging
import pandas as pd
from .database_schema import DatabaseManager, Stock, Crypto, Forex

logger = logging.getLogger(__name__)


class DataLoader:
    """Load financial data into database"""
    
    def __init__(self, db_path: str):
        self.db_manager = DatabaseManager(db_path)
        self.db_manager.connect()
        self.db_manager.create_tables()
        
    def load_stocks(self, df: pd.DataFrame) -> int:
        """
        Load stock data to database
        
        Args:
            df: DataFrame with stock data
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty stock data DataFrame, skipping load")
            return 0
            
        logger.info(f"Loading {len(df)} stock records")
        
        try:
            df.to_sql(
                'stocks',
                self.db_manager.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} stock records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load stock data: {e}")
            raise
            
    def load_crypto(self, df: pd.DataFrame) -> int:
        """
        Load crypto data to database
        
        Args:
            df: DataFrame with crypto data
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty crypto data DataFrame, skipping load")
            return 0
            
        logger.info(f"Loading {len(df)} crypto records")
        
        try:
            df.to_sql(
                'crypto',
                self.db_manager.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} crypto records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load crypto data: {e}")
            raise
            
    def load_forex(self, df: pd.DataFrame) -> int:
        """
        Load forex data to database
        
        Args:
            df: DataFrame with forex data
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty forex data DataFrame, skipping load")
            return 0
            
        logger.info(f"Loading {len(df)} forex records")
        
        try:
            df.to_sql(
                'forex',
                self.db_manager.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df)} forex records")
            return len(df)
            
        except Exception as e:
            logger.error(f"Failed to load forex data: {e}")
            raise
            
    def get_record_counts(self) -> dict:
        """Get count of records in each table"""
        session = self.db_manager.get_session()
        
        try:
            stock_count = session.query(Stock).count()
            crypto_count = session.query(Crypto).count()
            forex_count = session.query(Forex).count()
            
            return {
                'stocks': stock_count,
                'crypto': crypto_count,
                'forex': forex_count
            }
        finally:
            session.close()
