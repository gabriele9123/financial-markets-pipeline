"""
Unit tests for financial markets pipeline
"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime


class TestExtractors:
    """Test extraction scripts"""
    
    @patch('requests.get')
    def test_crypto_extraction(self, mock_get):
        """Test crypto data extraction"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from extractors.crypto_extractor import CryptoExtractor
        
        mock_response = Mock()
        mock_response.json.return_value = [{
            'id': 'bitcoin',
            'symbol': 'btc',
            'name': 'Bitcoin',
            'current_price': 45000,
            'market_cap': 850000000000,
            'total_volume': 25000000000,
            'price_change_percentage_24h': 2.5
        }]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        extractor = CryptoExtractor()
        result = extractor.extract_crypto_details(['bitcoin'])
        
        assert result is not None
        assert len(result) == 1
        assert result[0]['id'] == 'bitcoin'


class TestTransformers:
    """Test transformation scripts"""
    
    def test_crypto_transformation(self):
        """Test crypto data transformation"""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from transformers.crypto_transformer import CryptoTransformer
        
        raw_data = [{
            'id': 'bitcoin',
            'symbol': 'btc',
            'name': 'Bitcoin',
            'current_price': 45000,
            'market_cap': 850000000000,
            'total_volume': 25000000000,
            'price_change_percentage_24h': 2.5
        }]
        
        transformer = CryptoTransformer()
        df = transformer.transform(raw_data)
        
        assert not df.empty
        assert len(df) == 1
        assert 'symbol' in df.columns
        assert df.iloc[0]['current_price'] == 45000


class TestLoaders:
    """Test loading scripts"""
    
    def test_database_creation(self):
        """Test database table creation"""
        import sys
        import os
        import tempfile
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
        
        from loaders.database_schema import DatabaseManager
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            manager = DatabaseManager(db_path)
            manager.connect()
            manager.create_tables()
            
            from sqlalchemy import inspect
            inspector = inspect(manager.engine)
            tables = inspector.get_table_names()
            
            assert 'stocks' in tables
            assert 'crypto' in tables
            assert 'forex' in tables
            
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)
