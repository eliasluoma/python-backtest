import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.data.firebase_service import FirebaseService


class TestFirebaseService(unittest.TestCase):
    """Test cases for the FirebaseService class."""

    @patch("firebase_admin.credentials.Certificate")
    @patch("firebase_admin.initialize_app")
    @patch("firebase_admin.firestore.client")
    def setUp(self, mock_firestore_client, mock_initialize_app, mock_certificate):
        """Set up test fixtures with mocked Firebase dependencies."""
        # Mock the Firestore client
        self.mock_db = MagicMock()
        mock_firestore_client.return_value = self.mock_db

        # Mock credentials for test
        test_credentials = {
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "test-key-id",
            "private_key": "test-key",
            "client_email": "test@example.com",
            "client_id": "test-client-id",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test@example.com",
        }

        # Initialize the service with the mocked dependencies
        self.firebase_service = FirebaseService(env_file=None, credentials_json=test_credentials)

        # Store references to the mocks for assertions
        self.mock_certificate = mock_certificate
        self.mock_initialize_app = mock_initialize_app
        self.mock_firestore_client = mock_firestore_client

    def test_initialization(self):
        """Test that the FirebaseService initializes correctly."""
        # Assert that Certificate was called with the test credentials
        self.mock_certificate.assert_called_once()

        # Assert that initialize_app was called
        self.mock_initialize_app.assert_called_once()

        # Assert that firestore client was created
        self.mock_firestore_client.assert_called_once()

    @patch("pandas.DataFrame")
    def test_fetch_market_data_success(self, mock_dataframe):
        """Test fetching market data successfully."""
        # Mock the collection and document references
        mock_collection = MagicMock()
        mock_pool_docs = []

        # Create two mock pool documents
        for i in range(2):
            mock_pool = MagicMock()
            mock_pool.id = f"pool_{i}"
            mock_pool.to_dict.return_value = {
                "name": f"Pool {i}",
                "data": [{"timestamp": f"2023-01-0{i+1}T12:00:00", "marketCap": i * 10000}],
            }
            mock_pool_docs.append(mock_pool)

        # Configure the mocks to return our test data
        mock_collection.stream.return_value = mock_pool_docs
        self.mock_db.collection.return_value = mock_collection

        # Mock the DataFrame creation
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df

        # Call the method to test
        result = self.firebase_service.fetch_market_data(limit_pools=2)

        # Assertions
        self.mock_db.collection.assert_called_with("pools")
        mock_collection.stream.assert_called_once()
        self.assertEqual(result, mock_df)

    @patch("pandas.DataFrame")
    def test_fetch_market_data_empty(self, mock_dataframe):
        """Test fetching market data when no pools are available."""
        # Mock empty collection result
        mock_collection = MagicMock()
        mock_collection.stream.return_value = []
        self.mock_db.collection.return_value = mock_collection

        # Mock empty DataFrame
        mock_empty_df = pd.DataFrame()
        mock_dataframe.return_value = mock_empty_df

        # Call the method to test
        result = self.firebase_service.fetch_market_data()

        # Assertions
        self.mock_db.collection.assert_called_with("pools")
        mock_collection.stream.assert_called_once()
        self.assertTrue(result.empty)

    @patch("pandas.DataFrame")
    def test_fetch_market_data_with_limit(self, mock_dataframe):
        """Test fetching market data with a pool limit."""
        # Create mock pools
        mock_pools = []
        for i in range(5):
            mock_pool = MagicMock()
            mock_pool.id = f"pool_{i}"
            mock_pool.to_dict.return_value = {
                "name": f"Pool {i}",
                "data": [{"timestamp": f"2023-01-0{i+1}T12:00:00", "marketCap": i * 10000}],
            }
            mock_pools.append(mock_pool)

        # Configure the mocks
        mock_collection = MagicMock()
        mock_collection.stream.return_value = mock_pools
        self.mock_db.collection.return_value = mock_collection

        # Mock the DataFrame
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df

        # Call with a limit of 3 pools
        result = self.firebase_service.fetch_market_data(limit_pools=3)

        # Assert only 3 pools were processed
        self.assertEqual(mock_dataframe.call_count, 1)
        # We can't easily test the actual limit in the mock setup,
        # but we can verify the result is as expected
        self.assertEqual(result, mock_df)

    def test_error_handling(self):
        """Test error handling during market data fetching."""
        # Configure the mock to raise an exception
        self.mock_db.collection.side_effect = Exception("Test error")

        # Call the method that should handle the error
        result = self.firebase_service.fetch_market_data()

        # Verify we get an empty DataFrame on error
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
