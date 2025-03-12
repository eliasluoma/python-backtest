import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data.firebase_service import FirebaseService


class TestFirebaseService(unittest.TestCase):
    """Test FirebaseService class."""

    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.credentials.Certificate")
    @patch("src.data.firebase_service.firestore")
    def setUp(self, mock_firestore, mock_cert, mock_init):
        """Set up test fixtures."""
        # Create a mock Firestore client
        mock_db = MagicMock()
        mock_firestore.client.return_value = mock_db

        # Set up mock collection references
        mock_collection = MagicMock()
        mock_db.collection.return_value = mock_collection

        # Create a mock document reference
        mock_pool_doc = MagicMock()
        mock_collection.list_documents.return_value = [mock_pool_doc]

        # Create a mock snapshots collection
        mock_snapshots_collection = MagicMock()
        mock_pool_doc.collection.return_value = mock_snapshots_collection

        # Set up mock query results
        mock_query = MagicMock()
        mock_snapshots_collection.stream.return_value = []  # Empty results by default
        mock_snapshots_collection.order_by.return_value = mock_query
        mock_snapshots_collection.where.return_value = mock_snapshots_collection
        mock_query.get.return_value = []

        # Set up pool ID
        mock_pool_doc.id = "test_pool_123"

        # Create test credentials
        test_credentials = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "test-private-key",
            "client_email": "test@example.com",
            "client_id": "test-client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40example.com",
        }

        # Initialize the service with the mock
        self.firebase_service = FirebaseService(env_file=None, credentials_json=test_credentials)

        # Store references to mocks for later use
        self.mock_db = mock_db
        self.mock_collection = mock_collection
        self.mock_pool_doc = mock_pool_doc
        self.mock_snapshots_collection = mock_snapshots_collection

    def test_initialization(self):
        """Test that the FirebaseService initializes correctly."""
        self.assertIsNotNone(self.firebase_service.db)

    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.credentials.Certificate")
    @patch("src.data.firebase_service.firestore")
    def test_fetch_market_data_success(self, mock_firestore, mock_cert, mock_init):
        """Test fetching market data successfully."""
        # Set up mock Firestore client
        mock_db = MagicMock()
        mock_firestore.client.return_value = mock_db

        # Set up mock collection
        mock_collection = MagicMock()
        mock_db.collection.return_value = mock_collection

        # Create mock pool documents
        mock_pool_doc1 = MagicMock()
        mock_pool_doc1.id = "pool1"
        mock_pool_doc2 = MagicMock()
        mock_pool_doc2.id = "pool2"
        mock_collection.list_documents.return_value = [mock_pool_doc1, mock_pool_doc2]

        # Set up mock snapshots for each pool
        mock_snapshots1 = MagicMock()
        mock_pool_doc1.collection.return_value = mock_snapshots1
        mock_snapshots1.where.return_value = mock_snapshots1

        mock_snapshots2 = MagicMock()
        mock_pool_doc2.collection.return_value = mock_snapshots2
        mock_snapshots2.where.return_value = mock_snapshots2

        # Create mock snapshot data
        mock_snapshot1 = MagicMock()
        mock_snapshot1.to_dict.return_value = {
            "timestamp": MagicMock(seconds=1615000000),
            "marketCap": 1000000,
            "holders": 100,
        }

        mock_snapshot2 = MagicMock()
        mock_snapshot2.to_dict.return_value = {
            "timestamp": MagicMock(seconds=1615000100),
            "marketCap": 1100000,
            "holders": 110,
        }

        # Set up mock streams with the sample data
        mock_snapshots1.stream.return_value = [mock_snapshot1, mock_snapshot2] * 30  # 60 snapshots
        mock_snapshots2.stream.return_value = [mock_snapshot1, mock_snapshot2] * 30  # 60 snapshots

        # Initialize the service with the new mocks
        service = FirebaseService(env_file=None, credentials_json={})
        service.db = mock_db

        # Test fetching market data with a limit
        result = service.fetch_market_data(limit_pools=2)

        # Check the result
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)
        self.assertIn("pool1", result)
        self.assertIn("pool2", result)
        self.assertIsInstance(result["pool1"], pd.DataFrame)
        self.assertIsInstance(result["pool2"], pd.DataFrame)
        self.assertEqual(len(result["pool1"]), 60)
        self.assertEqual(len(result["pool2"]), 60)

    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.credentials.Certificate")
    @patch("src.data.firebase_service.firestore")
    def test_fetch_market_data_with_limit(self, mock_firestore, mock_cert, mock_init):
        """Test fetching market data with a pool limit."""
        # Set up mock Firestore client
        mock_db = MagicMock()
        mock_firestore.client.return_value = mock_db

        # Set up mock collection
        mock_collection = MagicMock()
        mock_db.collection.return_value = mock_collection

        # Create mock pool documents
        pool_docs = [MagicMock() for _ in range(5)]
        for i, doc in enumerate(pool_docs):
            doc.id = f"pool{i+1}"
        mock_collection.list_documents.return_value = pool_docs

        # Set up mock snapshots for each pool
        for doc in pool_docs:
            mock_snapshots = MagicMock()
            doc.collection.return_value = mock_snapshots
            mock_snapshots.where.return_value = mock_snapshots

            # Create mock snapshot data
            mock_snapshot = MagicMock()
            mock_snapshot.to_dict.return_value = {
                "timestamp": MagicMock(seconds=1615000000),
                "marketCap": 1000000,
                "holders": 100,
            }

            # Set up mock streams with the sample data
            mock_snapshots.stream.return_value = [mock_snapshot] * 60  # 60 snapshots

        # Initialize the service with the new mocks
        service = FirebaseService(env_file=None, credentials_json={})
        service.db = mock_db

        # Test fetching market data with a limit
        result = service.fetch_market_data(limit_pools=3)

        # Check the result
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 3)
        self.assertIn("pool1", result)
        self.assertIn("pool2", result)
        self.assertIn("pool3", result)

    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.credentials.Certificate")
    @patch("src.data.firebase_service.firestore")
    def test_fetch_market_data_empty(self, mock_firestore, mock_cert, mock_init):
        """Test fetching market data when no pools are available."""
        # Set up mock Firestore client
        mock_db = MagicMock()
        mock_firestore.client.return_value = mock_db

        # Set up mock collection with no documents
        mock_collection = MagicMock()
        mock_db.collection.return_value = mock_collection
        mock_collection.list_documents.return_value = []

        # Initialize the service with the new mocks
        service = FirebaseService(env_file=None, credentials_json={})
        service.db = mock_db

        # Test fetching market data
        result = service.fetch_market_data()

        # Check the result
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.credentials.Certificate")
    @patch("src.data.firebase_service.firestore")
    def test_error_handling(self, mock_firestore, mock_cert, mock_init):
        """Test error handling during market data fetching."""
        # Set up mock Firestore client
        mock_db = MagicMock()
        mock_firestore.client.return_value = mock_db

        # Set up mock collection to raise an exception
        mock_collection = MagicMock()
        mock_db.collection.return_value = mock_collection
        mock_collection.list_documents.side_effect = Exception("Test error")

        # Initialize the service with the new mocks
        service = FirebaseService(env_file=None, credentials_json={})
        service.db = mock_db

        # Test fetching market data with an error
        result = service.fetch_market_data()

        # Check the result (should be an empty dict)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 0)

    # Add more tests for other methods as needed


if __name__ == "__main__":
    unittest.main()
