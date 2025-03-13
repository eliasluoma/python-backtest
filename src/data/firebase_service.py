"""
Firebase service for Solana trading simulator.

This module provides functionality to connect to Firebase Firestore,
retrieve market data, and process it for backtesting.
"""

import os
import pandas as pd
import pytz  # type: ignore # Ignore "Library stubs not installed for pytz"
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, Optional, List
import logging
import time
import concurrent.futures

try:
    from src.utils.firebase_utils import (
        initialize_firebase,
        fetch_market_data_for_pool,
        get_pool_ids,
        preprocess_market_data,
    )
except ImportError:
    # Handle relative import for testing
    import sys

    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.utils.firebase_utils import (
        initialize_firebase,
        fetch_market_data_for_pool,
        get_pool_ids,
        preprocess_market_data,
    )

# Configure logging for this module
logger = logging.getLogger("FirebaseService")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class FirebaseService:
    """
    Professional Firebase service for Solana bot backtesting.
    Provides functionality to fetch and process market data from Firebase Firestore
    without storing data in local files.
    """

    def __init__(self, credential_path: Optional[str] = None):
        """
        Initialize FirebaseService with optional credential path.

        Args:
            credential_path: Path to Firebase credentials JSON file (optional)
        """
        # Set credential path in environment if provided
        if credential_path:
            os.environ["FIREBASE_KEY_FILE"] = credential_path

        # Load environment variables
        try:
            load_dotenv(".env.local")
            logger.info("Environment variables loaded from .env.local")
        except Exception as e:
            logger.warning(f"Could not load .env.local: {str(e)}")

        # Initialize Firebase connection using the utility function
        self.db = initialize_firebase()
        if self.db:
            logger.info("Firebase initialized successfully")
        else:
            logger.warning("Failed to initialize Firebase - some functionality may be limited")

    def fetch_market_data(
        self,
        min_data_points: int = 20,
        max_pools: int = 10,
        limit_per_pool: int = 100,
        pool_address: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch market data from Firebase.

        Args:
            min_data_points: Minimum number of data points required per pool
            max_pools: Maximum number of pools to fetch data for
            limit_per_pool: Maximum number of data points to fetch per pool
            pool_address: Optional specific pool address to fetch data for

        Returns:
            Dictionary mapping pool IDs to their respective DataFrames
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot fetch market data")
            return {}

        start_time = time.time()

        try:
            # If specific pool is requested
            if pool_address:
                logger.info(f"Fetching data for specific pool: {pool_address}")
                df = fetch_market_data_for_pool(
                    self.db, pool_address, limit=limit_per_pool, min_data_points=min_data_points
                )

                if df is not None:
                    df = preprocess_market_data(df)
                    result = {pool_address: df}
                else:
                    result = {}
            else:
                # Fetch all pools within limits
                pool_ids = get_pool_ids(self.db, limit=max_pools)
                logger.info(f"Found {len(pool_ids)} pools, fetching data...")

                result = {}
                for pool_id in pool_ids:
                    df = fetch_market_data_for_pool(
                        self.db, pool_id, limit=limit_per_pool, min_data_points=min_data_points
                    )

                    if df is not None:
                        df = preprocess_market_data(df)
                        result[pool_id] = df

            elapsed_time = time.time() - start_time
            logger.info(f"Fetched {len(result)} pools in {elapsed_time:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            return {}

    def fetch_recent_market_data(
        self, hours_back: int = 24, min_data_points: int = 20, max_pools: int = 10
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch recent market data within the specified time range.

        Args:
            hours_back: Number of hours to look back
            min_data_points: Minimum number of data points required per pool
            max_pools: Maximum number of pools to fetch

        Returns:
            Dictionary mapping pool IDs to their respective DataFrames
        """
        logger.info(f"Fetching recent market data from the past {hours_back} hours")

        # Fetch all data first
        all_data = self.fetch_market_data(min_data_points=min_data_points, max_pools=max_pools)

        if not all_data:
            return {}

        # Filter for recent data
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        result = {}
        for pool_id, df in all_data.items():
            if "timestamp" in df.columns:
                recent_df = df[df["timestamp"] >= cutoff_time]
                if len(recent_df) >= min_data_points:
                    result[pool_id] = recent_df

        logger.info(f"Found {len(result)} pools with recent data")
        return result

    def get_available_pools(self, limit: Optional[int] = None) -> List[str]:
        """
        Get a list of available pool IDs.

        Args:
            limit: Maximum number of pool IDs to return (None means no limit)

        Returns:
            List of pool IDs
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot get available pools")
            return []

        try:
            return get_pool_ids(self.db, limit=limit)
        except Exception as e:
            logger.error(f"Error getting available pools: {str(e)}")
            return []

    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess the market data for backtesting

        Args:
            df: Raw market data DataFrame

        Returns:
            Preprocessed DataFrame ready for backtesting
        """
        logger.info("Preprocessing market data...")

        # Ensure timestamp is in datetime format
        if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Sort by pool and timestamp
        df = df.sort_values(["poolAddress", "timestamp"])

        # Fill missing values with appropriate defaults
        # Note: These column names should match what's expected by simulators
        numeric_columns = [
            "marketCap",
            "holdersCount",
            "marketCapChange5s",
            "marketCapChange10s",
            "marketCapChange30s",
            "marketCapChange60s",
            "buyVolume5s",
            "netVolume5s",
            "priceChangePercent",
        ]

        # Fill NA values with 0 for numeric columns
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Calculate additional derived metrics if needed
        # This will be expanded based on requirements

        logger.info(f"Preprocessing complete. DataFrame shape: {df.shape}")
        return df

    def fetch_pool_data(self, pool_address: str, collection_name: str = "marketContext") -> pd.DataFrame:
        """
        Fetch data for a specific pool

        Args:
            pool_address: The pool address to fetch data for
            collection_name: The Firestore collection name for market contexts

        Returns:
            DataFrame containing pool data
        """
        logger.info(f"Fetching data for pool {pool_address}")

        if not self.db:
            raise RuntimeError("Firebase connection not initialized")

        try:
            # Get the pool document
            pool_doc = self.db.collection(collection_name).document(pool_address)

            # Fetch all market contexts for this pool
            contexts = pool_doc.collection("marketContexts").order_by("timestamp").get()

            pool_contexts = []
            for context in contexts:
                data = context.to_dict()

                # Convert string numbers to floats (same as in fetch_market_data)
                for key in [
                    "marketCap",
                    "athMarketCap",
                    "minMarketCap",
                    "maMarketCap10s",
                    "maMarketCap30s",
                    "maMarketCap60s",
                    "marketCapChange5s",
                    "marketCapChange10s",
                    "marketCapChange30s",
                    "marketCapChange60s",
                    "priceChangeFromStart",
                ]:
                    if key in data and isinstance(data[key], str):
                        try:
                            data[key] = float(data[key])
                        except (ValueError, TypeError):
                            data[key] = 0.0

                # Convert timestamp
                if "timestamp" in data:
                    try:
                        if isinstance(data["timestamp"], (int, float)):
                            data["timestamp"] = datetime.fromtimestamp(data["timestamp"] / 1000).replace(
                                tzinfo=pytz.UTC
                            )
                        elif isinstance(data["timestamp"], str):
                            data["timestamp"] = datetime.fromtimestamp(float(data["timestamp"]) / 1000).replace(
                                tzinfo=pytz.UTC
                            )
                    except Exception:
                        # Skip invalid timestamps
                        continue

                data["poolAddress"] = pool_address
                pool_contexts.append(data)

            if not pool_contexts:
                logger.warning(f"No data found for pool {pool_address}")
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(pool_contexts)
            logger.info(f"Loaded {len(df)} data points for pool {pool_address}")

            # Return preprocessed data
            return self.preprocess_data(df)

        except Exception as e:
            logger.error(f"Error fetching data for pool {pool_address}: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error

    def get_pools_datapoints_counts(self, pool_ids: List[str]) -> Dict[str, int]:
        """
        Get the number of datapoints for multiple pools efficiently without fetching all data.
        
        Args:
            pool_ids: List of pool IDs to check
            
        Returns:
            Dict mapping pool_id to datapoint count
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot get datapoints counts")
            return {}
            
        result = {}
        
        # Check if list is empty
        if not pool_ids:
            return result
            
        try:
            # Process pools in batches of 10 for efficiency (adjust based on Firebase rate limits)
            batch_size = 20  # Increased batch size for faster processing (adjust if rate limit issues occur)
            logger.info(f"Processing {len(pool_ids)} pools in batches of {batch_size}")
            
            for i in range(0, len(pool_ids), batch_size):
                batch_ids = pool_ids[i:i+batch_size]
                logger.debug(f"Processing batch {i//batch_size + 1} with {len(batch_ids)} pools")
                
                # Process batch with some delay between pools to avoid rate limiting
                for pool_id in batch_ids:
                    try:
                        count = self._get_single_pool_datapoints_count(pool_id)
                        result[pool_id] = count
                        
                        # Small delay to avoid Firebase rate limits
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error getting datapoints count for pool {pool_id}: {e}")
                        result[pool_id] = 0
                
                # Log progress periodically
                if (i//batch_size) % 5 == 0 or i + batch_size >= len(pool_ids):
                    logger.info(f"Processed {min(i+batch_size, len(pool_ids))} of {len(pool_ids)} pools")
                
            return result
            
        except Exception as e:
            logger.error(f"Error getting datapoints counts: {e}")
            return result  # Return partial results even on error

    def _get_single_pool_datapoints_count(self, pool_id: str) -> int:
        """
        Internal method to get the number of datapoints for a single pool.
        First tries to find the count directly from document fields, then falls back to counting documents.
        
        Args:
            pool_id: Pool ID to check
            
        Returns:
            Integer count of datapoints
        """
        try:
            logger.debug(f"Starting datapoint calculation for pool {pool_id}")
            
            # Now that we know the exact path, check it first: /marketContext/[pool-id]/marketContexts/
            logger.debug(f"Checking exact path directly /marketContext/{pool_id}/marketContexts/")
            try:
                # This refers directly to the marketContexts subcollection under the document
                collection_ref = self.db.collection("marketContext").document(pool_id).collection("marketContexts")
                
                # First try to fetch one document to make sure the collection exists
                try:
                    # Fetch one document from the collection
                    docs = list(collection_ref.limit(1).stream())
                    if docs and len(docs) > 0:
                        logger.debug(f"Found document at path /marketContext/{pool_id}/marketContexts/: {docs[0].id}")
                        
                        # Now that we know the collection exists, try to count the documents
                        try:
                            # Try count API if available
                            if hasattr(collection_ref, "count") and callable(getattr(collection_ref, "count")):
                                count_query = collection_ref.count()
                                count_result = count_query.get()
                                if hasattr(count_result, "__len__") and len(count_result) > 0 and hasattr(count_result[0], "value"):
                                    data_count = count_result[0].value
                                    logger.debug(f"Found {data_count} datapoints for pool {pool_id} from path /marketContext/{pool_id}/marketContexts/ using count API")
                                    return data_count
                        except Exception as e:
                            logger.debug(f"Count API failed, switching to alternative calculation: {e}")
                        
                        # If count API doesn't work, fetch all documents and count them (limited amount)
                        docs_all = list(collection_ref.limit(5000).stream())
                        data_count = len(docs_all)
                        logger.debug(f"Found {data_count} datapoints for pool {pool_id} from path /marketContext/{pool_id}/marketContexts/ using stream method")
                        
                        # Show the first document's content for debugging
                        if data_count > 0:
                            first_doc = docs_all[0].to_dict()
                            logger.debug(f"First datapoint information: {first_doc}")
                            
                        # If datapoints were found, return the count
                        return data_count
                    else:
                        logger.debug(f"No documents found at path /marketContext/{pool_id}/marketContexts/")
                except Exception as e:
                    logger.debug(f"Error fetching documents from path /marketContext/{pool_id}/marketContexts/: {e}")
            except Exception as e:
                logger.debug(f"Error checking path /marketContext/{pool_id}/marketContexts/: {e}")
                
            # Try also to fetch a specific example directly
            test_path = f"marketContext/{pool_id}/marketContexts/marketContext_1741819702"
            logger.debug(f"Trying to fetch a direct example document: {test_path}")
            try:
                # Separate the path parts
                parts = test_path.split('/')
                if len(parts) >= 4:  # At least 4 parts: collection/document/subcollection/document
                    collection_name = parts[0]
                    document_id = parts[1]
                    subcollection_name = parts[2]
                    subdocument_id = parts[3]
                    
                    # Fetch the document
                    doc_ref = self.db.collection(collection_name).document(document_id)
                    subdoc_ref = doc_ref.collection(subcollection_name).document(subdocument_id)
                    doc = subdoc_ref.get()
                    
                    if doc.exists:
                        logger.debug(f"Found direct example document: {test_path}")
                        logger.debug(f"Document content: {doc.to_dict()}")
                        # If one document was found, the path is likely correct and we can try to count all
                        collection_ref = doc_ref.collection(subcollection_name)
                        docs_all = list(collection_ref.limit(5000).stream())
                        data_count = len(docs_all)
                        logger.debug(f"Found {data_count} datapoints for pool {pool_id} from path /{collection_name}/{document_id}/{subcollection_name}/ using stream method")
                        return data_count
                    else:
                        logger.debug(f"Example document not found: {test_path}")
            except Exception as e:
                logger.debug(f"Error fetching example document {test_path}: {e}")
            
            # 3. Specifically check the "marketContextStatus" collection
            
            # If we didn't find data from the exact path, use the older search method
            pool_doc_ref = None
            found_in = None
            
            # 1. Check from "marketContext" collection (singular form)
            try:
                pool_doc_ref = self.db.collection("marketContext").document(pool_id)
                pool_doc = pool_doc_ref.get()
                if pool_doc.exists:
                    found_in = "marketContext"
                    logger.debug(f"Pool {pool_id} found in 'marketContext' collection")
                else:
                    logger.debug(f"Pool {pool_id} not found in 'marketContext' collection")
            except Exception as e:
                logger.debug(f"Error when checking pool {pool_id} in marketContext collection: {e}")
            
            # 2. If not found, check from "marketContexts" collection (plural form)
            if not found_in:
                try:
                    pool_doc_ref = self.db.collection("marketContexts").document(pool_id)
                    pool_doc = pool_doc_ref.get()
                    if pool_doc.exists:
                        found_in = "marketContexts"
                        logger.debug(f"Pool {pool_id} found in 'marketContexts' collection")
                    else:
                        logger.debug(f"Pool {pool_id} not found in 'marketContexts' collection")
                except Exception as e:
                    logger.debug(f"Error when checking pool {pool_id} in marketContexts collection: {e}")
            
            # 3. Specifically check the "marketContextStatus" collection
            if not found_in:
                try:
                    pool_doc_ref = self.db.collection("marketContextStatus").document(pool_id)
                    pool_doc = pool_doc_ref.get()
                    if pool_doc.exists:
                        found_in = "marketContextStatus"
                        logger.debug(f"Pool {pool_id} found in 'marketContextStatus' collection")
                        
                        # Check if the document has a dataPointCount field
                        doc_data = pool_doc.to_dict()
                        if doc_data and 'dataPointCount' in doc_data:
                            data_count = doc_data['dataPointCount']
                            logger.debug(f"Pool {pool_id} dataPointCount: {data_count} (directly from marketContextStatus document)")
                            return data_count
                            
                        # Check other possible fields referring to datapoints
                        possible_fields = ['totalDataPoints', 'pointCount', 'dataPoints', 'count']
                        for field in possible_fields:
                            if doc_data and field in doc_data:
                                data_count = doc_data[field]
                                logger.debug(f"Pool {pool_id} {field}: {data_count} (directly from marketContextStatus document)")
                                return data_count
                                
                        # Check lastUpdate object fields
                        if doc_data and 'lastUpdate' in doc_data and isinstance(doc_data['lastUpdate'], dict):
                            last_update = doc_data['lastUpdate']
                            if 'dataPointCount' in last_update:
                                data_count = last_update['dataPointCount']
                                logger.debug(f"Pool {pool_id} lastUpdate.dataPointCount: {data_count}")
                                return data_count
                            # Also check totalPoints
                            if 'totalPoints' in last_update:
                                data_count = last_update['totalPoints']
                                logger.debug(f"Pool {pool_id} lastUpdate.totalPoints: {data_count}")
                                return data_count
                            # Also check pointCount
                            if 'pointCount' in last_update:
                                data_count = last_update['pointCount']
                                logger.debug(f"Pool {pool_id} lastUpdate.pointCount: {data_count}")
                                
                        # Print the entire document content for debugging    
                        logger.debug(f"Pool {pool_id} marketContextStatus document content: {doc_data}")
                        
                    else:
                        logger.debug(f"Pool {pool_id} not found in 'marketContextStatus' collection")
                except Exception as e:
                    logger.debug(f"Error when checking pool {pool_id} in marketContextStatus collection: {e}")
            
            # 4. If not found in previous collections, try to search in nested collections
            if not found_in:
                try:
                    logger.debug(f"Searching for pool {pool_id} in other collections")
                    collections = list(self.db.collections())
                    logger.debug(f"Firebase has {len(collections)} collections: {[c.id for c in collections]}")
                    
                    for collection in collections:
                        if collection.id not in ["marketContext", "marketContexts", "marketContextStatus"]:
                            # Try if the pool is in this collection
                            try:
                                doc_ref = collection.document(pool_id)
                                doc = doc_ref.get()
                                if doc.exists:
                                    logger.debug(f"Pool {pool_id} document found in collection {collection.id}")
                                    
                                    # Check if the document contains datapoint count
                                    doc_data = doc.to_dict()
                                    if doc_data and 'dataPointCount' in doc_data:
                                        data_count = doc_data['dataPointCount']
                                        logger.debug(f"Pool {pool_id} dataPointCount: {data_count} (directly from {collection.id} document)")
                                        return data_count
                                        
                                    # Try if this document has marketContexts subcollection
                                    market_col = doc_ref.collection("marketContexts")
                                    market_docs = list(market_col.limit(1).stream())
                                    if market_docs:
                                        pool_doc_ref = doc_ref
                                        found_in = f"{collection.id} (with marketContexts subcollection)"
                                        logger.debug(f"Pool {pool_id} found in collection {found_in}")
                                        break
                                    else:
                                        logger.debug(f"Pool {pool_id} found in collection {collection.id}, but marketContexts subcollection is empty")
                            except Exception as e:
                                logger.debug(f"Error when checking pool {pool_id} from collection {collection.id}: {e}")
                except Exception as e:
                    logger.debug(f"Error examining nested collections for pool {pool_id}: {e}")
            
            # If the pool is not found in any collection that would contain datapoints
            if not found_in:
                logger.debug(f"Pool {pool_id} not found in any collection that would contain datapoints")
                return 0
            
            # Fetch subcollection "marketContexts" - this has been modified to match the fetch_market_data_for_pool function
            logger.debug(f"Fetching subcollection 'marketContexts' for pool {pool_id} from collection {found_in}")
            collection_ref = pool_doc_ref.collection("marketContexts")
            
            # Support both count() API and fallback alternative
            try:
                # Some Firebase SDK versions have direct count() support
                if hasattr(collection_ref, "count") and callable(getattr(collection_ref, "count")):
                    logger.debug(f"Using count() API for pool {pool_id}")
                    count_query = collection_ref.count()
                    count_result = count_query.get()
                    
                    # Extract count from the result
                    logger.debug(f"Count API result: {count_result}")
                    if hasattr(count_result, "__len__") and len(count_result) > 0 and hasattr(count_result[0], "value"):
                        data_count = count_result[0].value
                        logger.debug(f"Found {data_count} datapoints for pool {pool_id} from collection {found_in} using count API")
                        return data_count
                    else:
                        logger.debug(f"Count API returned empty or invalid result: {count_result}")
                else:
                    logger.debug(f"Count API is not available for pool {pool_id}, using alternative method")
            except Exception as e:
                logger.debug(f"Count API failed for pool {pool_id}, reason: {e}")
            
            # Alternative method: Fetch limited number of documents and check the count
            try:
                logger.debug(f"Using document stream method for pool {pool_id}")
                # Fetch only 5000 documents to keep the query time reasonable
                docs = list(collection_ref.limit(5000).stream())
                data_count = len(docs)
                logger.debug(f"Found {data_count} datapoints for pool {pool_id} from collection {found_in} using stream method")
                
                # If datapoints were found, print information about the first document
                if data_count > 0:
                    first_doc = docs[0].to_dict()
                    logger.debug(f"First datapoint information: {first_doc}")
                    
                return data_count
            except Exception as e:
                logger.error(f"Error in stream method for pool {pool_id}: {e}")
                return 0
            
        except Exception as e:
            logger.error(f"Total error when calculating datapoints for pool {pool_id}: {e}")
            return 0

    def get_first_and_last_document_id(self, pool_id: str) -> tuple:
        """
        Fetches the first and last market context document IDs for a pool.
        This method is used to estimate the number of datapoints without fetching all datapoints.
        
        Args:
            pool_id: Pool ID
            
        Returns:
            tuple: (first document ID, last document ID) or (None, None) if no documents are found
        """
        if not self.db:
            logger.error("Firebase not initialized, cannot get document IDs")
            return None, None
            
        try:
            # First try to fetch from marketContext/{pool_id}/marketContexts collection
            contexts_ref = self.db.collection("marketContext").document(pool_id).collection("marketContexts")
            
            # Check if data exists at this path
            test_docs = list(contexts_ref.limit(1).stream())
            
            # If not found, try directly the marketContext collection
            if not test_docs:
                logger.debug(f"No data found at path marketContext/{pool_id}/marketContexts, trying directly marketContext")
                # Try marketContext collection, where IDs can be in the format {pool_id}_timestamp
                contexts_ref = self.db.collection("marketContext")
                
                # Get the first document in chronological order (oldest first)
                # Find documents where ID starts with poolID
                prefix = f"{pool_id}_"
                first_doc_query = contexts_ref.where("__name__", ">=", prefix).where("__name__", "<=", prefix + "\uf8ff").order_by("__name__").limit(1)
                first_docs = list(first_doc_query.stream())
                
                if not first_docs:
                    logger.debug(f"No first document found for pool {pool_id} directly from marketContext collection")
                    return None, None
                
                first_doc = first_docs[0]
                first_id = first_doc.id
                
                # Get the last document (newest)
                try:
                    from google.cloud.firestore_v1 import Query
                    last_doc_query = contexts_ref.where("__name__", ">=", prefix).where("__name__", "<=", prefix + "\uf8ff").order_by("__name__", direction=Query.DESCENDING).limit(1)
                except (ImportError, AttributeError):
                    # If it's not available, use the string directly
                    last_doc_query = contexts_ref.order_by("timestamp", direction="DESCENDING").limit(1)
                    
                last_docs = list(last_doc_query.stream())
                
                if not last_docs:
                    logger.debug(f"No last document found for pool {pool_id}")
                    return first_id, None
                
                last_doc = last_docs[0]
                last_id = last_doc.id
                
                # Try to get timestamp values to improve the estimate
                first_timestamp = None
                last_timestamp = None
                
                try:
                    if 'timestamp' in first_doc.to_dict():
                        first_timestamp = first_doc.to_dict()['timestamp']
                    if 'timestamp' in last_doc.to_dict():
                        last_timestamp = last_doc.to_dict()['timestamp']
                        
                    # Log timestamp differences for debugging
                    if first_timestamp and last_timestamp:
                        time_diff = last_timestamp - first_timestamp
                        logger.debug(f"Pool {pool_id} timeline: {first_timestamp} - {last_timestamp}, difference {time_diff}")
                except Exception as e:
                    logger.debug(f"Failed to retrieve timestamp values: {e}")
                
                logger.debug(f"Documents found for pool {pool_id} between {first_id} - {last_id}")
                return first_id, last_id
            
            # Continue with the original path marketContext/{pool_id}/marketContexts
            # Get the first document in chronological order (oldest first)
            # Use the timestamp field as it's likely already indexed
            first_doc_query = contexts_ref.order_by("timestamp").limit(1)
            first_docs = list(first_doc_query.stream())
            
            if not first_docs:
                logger.debug(f"No first document found for pool {pool_id}")
                return None, None
            
            first_doc = first_docs[0]
            first_id = first_doc.id
            
            # Get the last document (newest)
            try:
                # First try the google.cloud.firestore library method
                from google.cloud.firestore_v1 import Query
                last_doc_query = contexts_ref.order_by("timestamp", direction=Query.DESCENDING).limit(1)
            except (ImportError, AttributeError):
                # If it's not available, use the string directly
                last_doc_query = contexts_ref.order_by("timestamp", direction="DESCENDING").limit(1)
                
            last_docs = list(last_doc_query.stream())
            
            if not last_docs:
                logger.debug(f"No last document found for pool {pool_id}")
                return first_id, None
            
            last_doc = last_docs[0]
            last_id = last_doc.id
            
            # Try to get timestamp values to improve the estimate
            first_timestamp = None
            last_timestamp = None
            
            try:
                if 'timestamp' in first_doc.to_dict():
                    first_timestamp = first_doc.to_dict()['timestamp']
                if 'timestamp' in last_doc.to_dict():
                    last_timestamp = last_doc.to_dict()['timestamp']
                    
                # Log timestamp differences for debugging
                if first_timestamp and last_timestamp:
                    time_diff = last_timestamp - first_timestamp
                    logger.debug(f"Pool {pool_id} timeline: {first_timestamp} - {last_timestamp}, difference {time_diff}")
            except Exception as e:
                logger.debug(f"Failed to retrieve timestamp values: {e}")
            
            logger.debug(f"Documents found for pool {pool_id} between {first_id} - {last_id}")
            return first_id, last_id
            
        except Exception as e:
            logger.error(f"Error retrieving document IDs for pool {pool_id}: {e}")
            return None, None
