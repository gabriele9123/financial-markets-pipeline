"""
Database Schema Definitions
Defines tables for financial market data
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class Stock(Base):
    """Stock market data model"""
    __tablename__ = 'stocks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10))
    price = Column(Float)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    latest_trading_day = Column(String(20))
    previous_close = Column(Float)
    change = Column(Float)
    change_percent = Column(Float)
    timestamp = Column(DateTime)
    extracted_at = Column(DateTime)


class Crypto(Base):
    """Cryptocurrency data model"""
    __tablename__ = 'crypto'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10))
    name = Column(String(100))
    current_price = Column(Float)
    market_cap = Column(Float)
    total_volume = Column(Float)
    price_change_24h = Column(Float)
    timestamp = Column(DateTime)
    extracted_at = Column(DateTime)


class Forex(Base):
    """Foreign exchange rate data model"""
    __tablename__ = 'forex'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    base_currency = Column(String(3))
    target_currency = Column(String(3))
    exchange_rate = Column(Float)
    rate_date = Column(String(20))
    timestamp = Column(DateTime)
    extracted_at = Column(DateTime)


class DatabaseManager:
    """Manage database connections and operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = None
        self.Session = None
        
    def connect(self):
        """Create database connection"""
        logger.info(f"Connecting to database: {self.db_path}")
        self.engine = create_engine(f'sqlite:///{self.db_path}')
        self.Session = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Create all tables if they don't exist"""
        logger.info("Creating database tables")
        Base.metadata.create_all(self.engine)
        logger.info("Tables created successfully")
        
    def get_session(self):
        """Get a new database session"""
        if not self.Session:
            self.connect()
        return self.Session()
