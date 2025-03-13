"""
SQLite-based Cache Service for Pool Data

This module provides a caching mechanism for pool data using SQLite as the backend.
All field names match the REQUIRED_FIELDS list from pool_analyzer.py for full consistency.
"""

import sqlite3
import json
import shutil
import logging
import signal
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Union, Optional, Any, Tuple

import pandas as pd
import numpy as np
from functools import lru_cache

# Import field utilities and constants
from src.utils.field_utils import (
    normalize_dataframe_columns,
    snake_to_camel,
    camel_to_snake,
)
from constants.fields import FIELD_ADDITIONAL_DATA

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that can handle Timestamp objects and other special types."""

    def default(self, obj):
        # Handle datetime objects
        if hasattr(obj, "isoformat"):
            return obj.isoformat()

        # Handle Timestamp objects (from Firebase)
        if hasattr(obj, "seconds") and hasattr(obj, "nanoseconds"):
            return datetime.fromtimestamp(obj.seconds + obj.nanoseconds / 1e9).isoformat()

        # Handle NumPy types
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)

        # Let the base class handle it or raise TypeError
        return super().default(obj)


class DataCacheService:
    """SQLite-based cache service for pool data with consistent field naming"""

    def __init__(
        self, db_path: Union[str, Path], schema_path: Optional[Union[str, Path]] = None, memory_cache_size: int = 100
    ):
        """
        Initialize the cache service.

        Args:
            db_path: Path to the SQLite database file
            schema_path: Path to the SQL schema file (if None, use default schema)
            memory_cache_size: Maximum number of pools to cache in memory
        """
        # Convert paths to Path objects
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path) if schema_path else Path(__file__).parent / "schema.sql"

        # Create cache directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize the database with schema
        self._init_database()

        # Set up memory cache
        self.memory_cache_size = memory_cache_size
        self.memory_cache: Dict[str, pd.DataFrame] = {}

        # Init/update memory cache decorator
        self.get_from_memory = lru_cache(maxsize=memory_cache_size)(self._get_from_memory)

        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        logger.info(f"Cache service initialized with database at {self.db_path}")

    def _init_database(self) -> None:
        """Initialize the SQLite database with schema"""
        # Check if database file exists
        db_exists = self.db_path.exists()

        # Connect to database
        conn = sqlite3.connect(str(self.db_path))

        try:
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")

            if not db_exists:
                logger.info(f"Creating new database at {self.db_path}")

                # Read schema file
                if not self.schema_path.exists():
                    raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

                with open(self.schema_path, "r") as f:
                    schema_sql = f.read()

                # Execute schema script
                conn.executescript(schema_sql)
                logger.info("Database initialized with schema")

            # Check schema version
            cursor = conn.execute("SELECT version FROM schema_version WHERE id = 1")
            version = cursor.fetchone()

            if not version:
                # Initialize schema version
                conn.execute("INSERT INTO schema_version (id, version, updated_at) VALUES (1, 2, datetime('now'))")

            logger.info(f"Connected to database (schema version: {version[0] if version else 2})")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()

    def _get_from_memory(
        self, pool_id: str, min_time: Optional[str] = None, max_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Empty function for creating cache keys - actual retrieval is done elsewhere

        This function is decorated with lru_cache to maintain the cache
        """
        # This function is only used for its cache functionality
        # It's called to check if a key exists in the cache, but the data is stored separately
        return pd.DataFrame()

    def get_pool_data(
        self, pool_id: str, min_timestamp: Optional[datetime] = None, max_timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Get pool data from cache, falling back to SQLite if not in memory.

        Args:
            pool_id: The pool ID to fetch data for
            min_timestamp: Optional minimum timestamp to filter data
            max_timestamp: Optional maximum timestamp to filter data

        Returns:
            DataFrame containing pool data
        """
        # Convert timestamps to strings for cache key
        min_ts_str = min_timestamp.isoformat() if min_timestamp else None
        max_ts_str = max_timestamp.isoformat() if max_timestamp else None

        # Check memory cache first
        cache_key = f"{pool_id}_{min_ts_str}_{max_ts_str}"

        # Call the cached function to register the cache key
        self.get_from_memory(pool_id, min_ts_str, max_ts_str)

        # Check if we have the data in memory
        if cache_key in self.memory_cache:
            logger.debug(f"Cache hit for {pool_id} in memory")
            return self.memory_cache[cache_key]

        # Get from SQLite
        logger.debug(f"Cache miss for {pool_id} in memory, fetching from SQLite")
        df = self._get_from_sqlite(pool_id, min_timestamp, max_timestamp)

        # Store in memory cache if not empty
        if not df.empty:
            self.memory_cache[cache_key] = df

        return df

    def _get_from_sqlite(
        self, pool_id: str, min_timestamp: Optional[datetime] = None, max_timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Get pool data from SQLite database"""
        # Prepare query - Select all columns from the market_data table
        query = """
        SELECT *
        FROM market_data
        WHERE poolAddress = ?
        """

        # Add timestamp filters if provided
        params = [pool_id]

        if min_timestamp:
            query += " AND timestamp >= ?"
            min_ts_str = min_timestamp.isoformat() if isinstance(min_timestamp, datetime) else min_timestamp
            params.append(min_ts_str)

        if max_timestamp:
            query += " AND timestamp <= ?"
            max_ts_str = max_timestamp.isoformat() if isinstance(max_timestamp, datetime) else max_timestamp
            params.append(max_ts_str)

        query += " ORDER BY timestamp"

        # Execute query
        try:
            conn = sqlite3.connect(str(self.db_path))
            # Convert timestamp from text to datetime
            conn.create_function("datetime", 1, lambda x: str(datetime.fromisoformat(x)) if x else None)

            df = pd.read_sql_query(query, conn, params=params)

            # Process JSON columns if present
            if not df.empty and FIELD_ADDITIONAL_DATA in df.columns:
                # Parse additional_data
                for i, row in df.iterrows():
                    if row[FIELD_ADDITIONAL_DATA] and row[FIELD_ADDITIONAL_DATA] != "{}":
                        try:
                            add_data = json.loads(row[FIELD_ADDITIONAL_DATA])
                            for key, value in add_data.items():
                                df.at[i, key] = value
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON in {FIELD_ADDITIONAL_DATA} for pool {pool_id}")

                # Drop the JSON column after extraction
                df = df.drop(FIELD_ADDITIONAL_DATA, axis=1)

                # Convert timestamp to datetime
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])

            return df

        except Exception as e:
            logger.error(f"Error fetching data from SQLite: {e}")
            return pd.DataFrame()
        finally:
            if "conn" in locals():
                conn.close()

    def update_pool_data(self, pool_id: str, df: pd.DataFrame, replace: bool = False) -> bool:
        """
        Update pool data in the cache.

        Args:
            pool_id: The pool ID to update
            df: DataFrame containing pool data
            replace: Whether to replace existing data (True) or append (False)

        Returns:
            True if successful, False otherwise
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for pool {pool_id}, skipping update")
            return False

        # Standardize column names to camelCase
        df = self._standardize_column_names(df)

        # Ensure timestamp column exists and is in datetime format
        if "timestamp" not in df.columns:
            logger.error(f"No timestamp column in DataFrame for pool {pool_id}")
            return False

        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Ensure DataFrame has poolAddress column with correct value
        if "poolAddress" not in df.columns:
            df["poolAddress"] = pool_id
            logger.info(f"Added poolAddress column for pool {pool_id}")

        # Ensure timestamp column is in ISO format
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["timestamp"] = df["timestamp"].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x))
            logger.debug(
                f"Converted timestamp column to ISO format, example: {df['timestamp'].iloc[0] if not df.empty else None}"
            )

        # Format other datetime fields as ISO strings
        datetime_columns = ["creationTime", "lastUpdated", "minTimestamp", "maxTimestamp"]
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                df[col] = df[col].apply(lambda x: x.isoformat() if hasattr(x, "isoformat") and pd.notnull(x) else None)
                logger.debug(f"Formatted {col} as ISO datetime strings")

        # Make sure timeFromStart is an integer
        if "timeFromStart" in df.columns:
            # Get original type for debugging
            orig_type = df["timeFromStart"].dtype
            logger.info(
                f"timeFromStart original type: {orig_type}, sample value: {df['timeFromStart'].iloc[0] if not df.empty else None}"
            )

            try:
                # First check for Firebase Timestamp objects
                if df["timeFromStart"].dtype == "object":
                    sample = df["timeFromStart"].iloc[0] if not df.empty else None
                    if hasattr(sample, "seconds") and hasattr(sample, "nanoseconds"):
                        logger.info("Converting timeFromStart from Firebase Timestamp objects to integer seconds")
                        df["timeFromStart"] = df["timeFromStart"].apply(
                            lambda x: (
                                int(x.seconds + x.nanoseconds / 1e9)
                                if hasattr(x, "seconds") and hasattr(x, "nanoseconds")
                                else x
                            )
                        )

                # Handle string values that might be timestamps
                df["timeFromStart"] = df["timeFromStart"].apply(
                    lambda x: pd.to_datetime(x).timestamp() if isinstance(x, str) and ":" in x else x
                )

                # Finally convert to integer
                df["timeFromStart"] = df["timeFromStart"].astype(float).astype(int)
                logger.info(
                    f"Successfully converted timeFromStart to INTEGER: {df['timeFromStart'].dtype}, sample: {df['timeFromStart'].iloc[0] if not df.empty else None}"
                )
            except Exception as e:
                logger.warning(f"Error converting timeFromStart to integer: {e}")
                # Handle conversion error: try a different approach
                try:
                    # Try to handle each value individually
                    def safe_convert_to_int(val):
                        if pd.isna(val) or val is None:
                            return 0
                        elif hasattr(val, "seconds") and hasattr(val, "nanoseconds"):
                            return int(val.seconds + val.nanoseconds / 1e9)
                        elif isinstance(val, str):
                            # Try to parse as timestamp if it looks like a date/time
                            if ":" in val:
                                try:
                                    return int(pd.to_datetime(val).timestamp())
                                except:
                                    return 0
                            # Otherwise try to convert string to int directly
                            try:
                                return int(float(val))
                            except:
                                return 0
                        else:
                            try:
                                return int(float(val))
                            except:
                                return 0

                    df["timeFromStart"] = df["timeFromStart"].apply(safe_convert_to_int)
                    logger.info(
                        f"Recovered timeFromStart conversion to INTEGER using safe conversion: {df['timeFromStart'].dtype}"
                    )
                except Exception as inner_e:
                    logger.error(f"Failed all attempts to convert timeFromStart to INTEGER: {inner_e}")
                    # Last resort: use 0 as a default value
                    df["timeFromStart"] = 0
                    logger.warning("Using 0 as default value for timeFromStart")

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Extract metadata - get these before converting timestamp to ISO format
        min_timestamp_val = df["timestamp"].min()
        max_timestamp_val = df["timestamp"].max()
        data_points = len(df)

        # Ensure min_timestamp and max_timestamp are properly formatted
        if hasattr(min_timestamp_val, "isoformat"):
            min_timestamp = min_timestamp_val.isoformat()
        else:
            min_timestamp = str(min_timestamp_val)

        if hasattr(max_timestamp_val, "isoformat"):
            max_timestamp = max_timestamp_val.isoformat()
        else:
            max_timestamp = str(max_timestamp_val)

        logger.debug(f"Min timestamp: {min_timestamp}, Max timestamp: {max_timestamp}")

        try:
            # Connect to database
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA foreign_keys = ON")

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            try:
                # Check if pool exists
                cursor = conn.execute("SELECT poolAddress FROM pools WHERE poolAddress = ?", (pool_id,))
                pool_exists = cursor.fetchone() is not None

                # Handle pool metadata
                if pool_exists:
                    if replace:
                        # Update with replacement
                        conn.execute(
                            """
                        UPDATE pools SET 
                            lastUpdated = datetime('now'),
                            dataPoints = ?,
                            minTimestamp = ?,
                            maxTimestamp = ?
                        WHERE poolAddress = ?
                        """,
                            (data_points, min_timestamp, max_timestamp, pool_id),
                        )
                    else:
                        # When appending, first remove any existing records with the same timestamps
                        # to avoid duplicates while preserving the unique constraint
                        timestamps = df["timestamp"].tolist()
                        timestamp_placeholders = ", ".join(["?" for _ in timestamps])
                        conn.execute(
                            f"DELETE FROM market_data WHERE poolAddress = ? AND timestamp IN ({timestamp_placeholders})",
                            [pool_id] + timestamps,
                        )
                        logger.debug(
                            f"Removed {len(timestamps)} existing records with matching timestamps for pool {pool_id}"
                        )
                else:
                    # Insert new pool
                    conn.execute(
                        """
                    INSERT INTO pools (
                        poolAddress, creationTime, lastUpdated, 
                        dataPoints, minTimestamp, maxTimestamp, metadata
                    ) VALUES (?, datetime('now'), datetime('now'), ?, ?, ?, ?)
                    """,
                        (pool_id, data_points, min_timestamp, max_timestamp, "{}"),
                    )

                # Handle market data
                if replace:
                    # Delete existing data
                    conn.execute("DELETE FROM market_data WHERE poolAddress = ?", (pool_id,))
                else:
                    # When appending, first remove any existing records with the same timestamps
                    # to avoid duplicates while preserving the unique constraint
                    timestamps = df["timestamp"].tolist()
                    timestamp_placeholders = ", ".join(["?" for _ in timestamps])
                    conn.execute(
                        f"DELETE FROM market_data WHERE poolAddress = ? AND timestamp IN ({timestamp_placeholders})",
                        [pool_id] + timestamps,
                    )
                    logger.debug(
                        f"Removed {len(timestamps)} existing records with matching timestamps for pool {pool_id}"
                    )

                # Get column names from table schema
                cursor = conn.execute("PRAGMA table_info(market_data)")
                db_columns = [row[1] for row in cursor.fetchall()]

                # Create a mapping of column names to their database counterparts, making sure
                # to handle case sensitivity issues (marketCapChange5s vs marketCapChange5S)
                column_map = {}

                for db_col in db_columns:
                    # Add direct mapping
                    column_map[db_col.lower()] = db_col

                    # Also map with common case inconsistencies
                    if db_col.lower().endswith("s"):
                        alt_col = db_col.lower().replace("s", "S")
                        column_map[alt_col] = db_col

                    # Add snake_case to camelCase mapping and vice versa
                    if "_" in db_col:  # It's snake_case in DB
                        camel_col = snake_to_camel(db_col)
                        column_map[camel_col.lower()] = db_col
                    else:  # It's camelCase in DB
                        snake_col = camel_to_snake(db_col)
                        column_map[snake_col.lower()] = db_col

                # Handle special trade fields separately
                trade_fields = [col for col in db_columns if col.startswith("trade_last")]
                for field in trade_fields:
                    # Map from camelCase format (tradeLast5Seconds.volume.buy) to DB format
                    parts = field.split("_")
                    if len(parts) >= 3:
                        # Create camelCase version: tradeLast5Seconds
                        camel_prefix = f"tradeLast{parts[1][4:]}Seconds"
                        # For fields like trade_last5Seconds_volume_buy
                        if len(parts) == 4:
                            camel_field = f"{camel_prefix}.{parts[2]}.{parts[3]}"
                        # For fields like trade_last5Seconds_tradeCount_buy_small
                        elif len(parts) == 5:
                            camel_field = f"{camel_prefix}.{parts[2]}.{parts[3]}.{parts[4]}"
                        else:
                            camel_field = field
                        column_map[camel_field.lower()] = field

                logger.debug(f"Column mapping created with {len(column_map)} entries")
                logger.debug(f"Sample mappings: {list(column_map.items())[:5]}")

                # Map DataFrame fields to DB columns based on our mapping
                # First, extract trade data from nested structures
                normalized_df = self.extract_trade_data_from_df(df)

                # Insert data row by row to ensure correct field mapping
                for _, row in normalized_df.iterrows():
                    # Create dictionaries for db fields and extra fields
                    db_fields = {}
                    extra_fields = {}

                    # Process each field in the row
                    for col in row.index:
                        value = row[col]

                        # Skip None values
                        if pd.isna(value) or value is None:
                            continue

                        # Direct match with DB columns
                        if col in db_columns:
                            db_fields[col] = value

                        # Check for match in our mapping (case-insensitive)
                        elif col.lower() in column_map:
                            db_col = column_map[col.lower()]
                            db_fields[db_col] = value

                        # For fields that don't have a direct mapping, try variations
                        else:
                            # Try S/s capitalization variations
                            col_s_variation = col.lower().replace("s", "S")
                            if col_s_variation in column_map:
                                db_col = column_map[col_s_variation]
                                db_fields[db_col] = value
                            else:
                                # Put in extras if no match found
                                extra_fields[col] = value

                    # Convert extra_fields to JSON string using our CustomJSONEncoder
                    if extra_fields:
                        try:
                            extra_data_json = json.dumps(extra_fields, cls=CustomJSONEncoder)
                            db_fields[FIELD_ADDITIONAL_DATA] = extra_data_json
                            logger.debug(f"Extra fields pushed to additional_data: {list(extra_fields.keys())}")
                        except TypeError as e:
                            logger.warning(f"Could not convert extra fields to JSON: {e}")
                            logger.debug(f"Problematic extra fields: {extra_fields}")
                            # Store a safe empty JSON object instead
                            db_fields[FIELD_ADDITIONAL_DATA] = "{}"

                    # Prepare SQL statement
                    cols = list(db_fields.keys())
                    placeholders = ", ".join(["?" for _ in cols])
                    column_names = ", ".join(cols)

                    # Get values ensuring they are SQLite-compatible
                    values = []
                    for col in cols:
                        val = db_fields[col]
                        try:
                            if isinstance(val, (list, dict)):
                                # Convert complex types to JSON
                                values.append(json.dumps(val, cls=CustomJSONEncoder))
                            elif hasattr(val, "seconds") and hasattr(val, "nanoseconds"):
                                # Handle Firebase Timestamp objects
                                dt = datetime.fromtimestamp(val.seconds + val.nanoseconds / 1e9)
                                values.append(dt.isoformat())
                            else:
                                # Append the value as is - SQLite will handle basic types
                                values.append(val)
                        except (TypeError, AttributeError) as e:
                            logger.warning(f"Error processing value for column {col}: {e}")
                            logger.debug(f"Value type: {type(val)}, Value: {val}")
                            # Store a safe default value based on column type
                            if "timestamp" in col.lower():
                                values.append(datetime.now().isoformat())
                            elif isinstance(val, (int, float, bool)):
                                # Convert to str to ensure SQLite compatibility
                                values.append(str(val))
                            else:
                                values.append(str(val))

                    # Log for debugging
                    logger.debug(f"Inserting row with columns: {cols}")
                    logger.debug(f"First 5 values: {values[:5] if len(values) >= 5 else values}")

                    # Insert data with REPLACE option to handle duplicate entries
                    conn.execute(f"INSERT OR REPLACE INTO market_data ({column_names}) VALUES ({placeholders})", values)

                # Update cache statistics
                conn.execute(
                    """
                UPDATE cache_stats SET
                    last_global_update = datetime('now'),
                    total_pools = (SELECT COUNT(*) FROM pools),
                    total_data_points = (SELECT COUNT(*) FROM market_data),
                    cache_size_bytes = (SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size())
                WHERE id = 1
                """
                )

                # Commit transaction
                conn.commit()

                # Clear memory cache for this pool
                self._clear_memory_cache(pool_id)

                logger.info(f"Successfully updated data for pool {pool_id} ({data_points} points)")
                return True

            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"Error updating pool {pool_id} data: {e}")
                return False

        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
        finally:
            if "conn" in locals():
                conn.close()

    def _clear_memory_cache(self, pool_id: Optional[str] = None) -> None:
        """
        Clear memory cache for a specific pool or all pools.

        Args:
            pool_id: Optional pool ID to clear cache for (if None, clear all)
        """
        if pool_id:
            # Clear specific pool from cache
            keys_to_remove = [k for k in self.memory_cache.keys() if k.startswith(f"{pool_id}_")]
            for k in keys_to_remove:
                del self.memory_cache[k]

            # Clear from decorated function cache
            self.get_from_memory.cache_clear()
            logger.debug(f"Cleared memory cache for pool {pool_id}")
        else:
            # Clear all
            self.memory_cache.clear()
            self.get_from_memory.cache_clear()
            logger.debug("Cleared all memory cache")

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to camelCase to match REQUIRED_FIELDS.

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with standardized column names
        """
        # Use the normalize_dataframe_columns utility to handle mixed naming conventions
        return normalize_dataframe_columns(df, target_convention="camel")

    def get_pool_ids(self, limit: int = 100, min_data_points: int = 0) -> List[str]:
        """
        Get list of available pool IDs from the cache.

        Args:
            limit: Maximum number of pool IDs to return
            min_data_points: Minimum number of data points required

        Returns:
            List of pool IDs
        """
        try:
            conn = sqlite3.connect(str(self.db_path))

            query = "SELECT poolAddress FROM pools"
            params = []

            if min_data_points > 0:
                query += " WHERE dataPoints >= ?"
                params.append(min_data_points)

            query += " ORDER BY dataPoints DESC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            result = [row[0] for row in cursor.fetchall()]

            return result

        except Exception as e:
            logger.error(f"Error getting pool IDs: {e}")
            return []
        finally:
            if "conn" in locals():
                conn.close()

    def filter_pools(
        self,
        min_data_points: int = 0,
        min_market_cap: Optional[float] = None,
        min_holders: Optional[int] = None,
        limit: int = 100,
    ) -> List[str]:
        """
        Filter pools based on criteria.

        Args:
            min_data_points: Minimum number of data points required
            min_market_cap: Minimum market cap (latest)
            min_holders: Minimum holders count (latest)
            limit: Maximum number of pools to return

        Returns:
            List of pool IDs meeting the criteria
        """
        try:
            conn = sqlite3.connect(str(self.db_path))

            # Start with basic query
            query = """
            SELECT p.poolAddress
            FROM pools p
            """

            params: List[Union[str, int, float]] = []
            where_clauses = []

            # Add data points filter
            if min_data_points > 0:
                where_clauses.append("p.dataPoints >= ?")
                params.append(min_data_points)

            # Add market cap filter if needed
            if min_market_cap is not None:
                query += """
                JOIN (
                    SELECT poolAddress, MAX(timestamp) as latest_ts
                    FROM market_data
                    GROUP BY poolAddress
                ) latest ON p.poolAddress = latest.poolAddress
                JOIN market_data md ON latest.poolAddress = md.poolAddress AND latest.latest_ts = md.timestamp
                """
                where_clauses.append("md.marketCap >= ?")
                params.append(float(min_market_cap))

                # Add holders filter if needed
                if min_holders is not None:
                    where_clauses.append("md.holdersCount >= ?")
                    params.append(min_holders)
            elif min_holders is not None:
                # Only holders filter, no market cap filter
                query += """
                JOIN (
                    SELECT poolAddress, MAX(timestamp) as latest_ts
                    FROM market_data
                    GROUP BY poolAddress
                ) latest ON p.poolAddress = latest.poolAddress
                JOIN market_data md ON latest.poolAddress = md.poolAddress AND latest.latest_ts = md.timestamp
                """
                where_clauses.append("md.holdersCount >= ?")
                params.append(min_holders)

            # Add WHERE clause if any filters
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Add limit
            query += " ORDER BY p.dataPoints DESC LIMIT ?"
            params.append(limit)

            # Execute query
            cursor = conn.execute(query, params)
            result = [row[0] for row in cursor.fetchall()]

            return result

        except Exception as e:
            logger.error(f"Error filtering pools: {e}")
            return []
        finally:
            if "conn" in locals():
                conn.close()

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary of cache statistics
        """
        try:
            conn = sqlite3.connect(str(self.db_path))

            # Get basic stats
            cursor = conn.execute(
                """
            SELECT 
                last_global_update, 
                total_pools, 
                total_data_points, 
                cache_size_bytes
            FROM cache_stats
            WHERE id = 1
            """
            )

            row = cursor.fetchone()
            if not row:
                return {"status": "error", "message": "No cache stats found"}

            stats = {
                "last_update": row[0],
                "total_pools": row[1],
                "total_data_points": row[2],
                "cache_size_mb": row[3] / (1024 * 1024),
                "memory_cache": {"size": len(self.memory_cache), "max_size": self.memory_cache_size},
            }

            # Get top 5 largest pools
            cursor = conn.execute(
                """
            SELECT poolAddress, dataPoints
            FROM pools
            ORDER BY dataPoints DESC
            LIMIT 5
            """
            )

            stats["largest_pools"] = [{"pool_id": row[0], "data_points": row[1]} for row in cursor.fetchall()]

            # Get general database info
            cursor = conn.execute("PRAGMA database_list")
            db_info = cursor.fetchone()
            stats["database_path"] = db_info[2] if db_info else str(self.db_path)

            return {"status": "success", "data": stats}

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"status": "error", "message": str(e)}
        finally:
            if "conn" in locals():
                conn.close()

    def clear_cache(self, older_than_days: Optional[int] = None) -> bool:
        """
        Clear the cache, optionally only for data older than specified days.

        Args:
            older_than_days: Optional days threshold (if None, clear all)

        Returns:
            True if successful, False otherwise
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA foreign_keys = ON")

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            try:
                if older_than_days is not None:
                    # Calculate cutoff date
                    cutoff = (datetime.now() - timedelta(days=older_than_days)).isoformat()

                    # Delete old data
                    conn.execute("DELETE FROM market_data WHERE timestamp < ?", (cutoff,))

                    # Update pool statistics
                    conn.execute(
                        """
                    UPDATE pools SET
                        dataPoints = (
                            SELECT COUNT(*) 
                            FROM market_data 
                            WHERE market_data.poolAddress = pools.poolAddress
                        ),
                        minTimestamp = (
                            SELECT MIN(timestamp) 
                            FROM market_data 
                            WHERE market_data.poolAddress = pools.poolAddress
                        ),
                        maxTimestamp = (
                            SELECT MAX(timestamp) 
                            FROM market_data 
                            WHERE market_data.poolAddress = pools.poolAddress
                        )
                    """
                    )

                    # Remove pools with no data
                    conn.execute("DELETE FROM pools WHERE dataPoints = 0")

                else:
                    # Clear all data
                    conn.execute("DELETE FROM market_data")
                    conn.execute("DELETE FROM pools")

                # Update cache stats
                conn.execute(
                    """
                UPDATE cache_stats SET
                    last_global_update = datetime('now'),
                    total_pools = (SELECT COUNT(*) FROM pools),
                    total_data_points = (SELECT COUNT(*) FROM market_data),
                    cache_size_bytes = (SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size())
                WHERE id = 1
                """
                )

                # Vacuum database to reclaim space
                conn.execute("VACUUM")

                # Commit transaction
                conn.commit()

                # Clear memory cache
                self._clear_memory_cache()

                if older_than_days is not None:
                    logger.info(f"Successfully cleared cache data older than {older_than_days} days")
                else:
                    logger.info("Successfully cleared all cache data")

                return True

            except Exception as e:
                # Rollback on error
                conn.rollback()
                logger.error(f"Error clearing cache: {e}")
                return False

        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
        finally:
            if "conn" in locals():
                conn.close()

    def backup_database(self, backup_path: Optional[Union[str, Path]] = None) -> Tuple[bool, str]:
        """
        Create a backup of the database file.

        Args:
            backup_path: Optional path for the backup file (if None, auto-generate)

        Returns:
            Tuple of (success, backup_path)
        """
        if not backup_path:
            # Auto-generate backup path with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.db_path.parent / f"backup_{timestamp}_{self.db_path.name}"
        else:
            backup_path = Path(backup_path)

        try:
            # Ensure backup directory exists
            backup_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy database file
            shutil.copy2(self.db_path, backup_path)

            logger.info(f"Successfully backed up database to {backup_path}")
            return True, str(backup_path)

        except Exception as e:
            logger.error(f"Error backing up database: {e}")
            return False, str(e)

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown."""
        logger.info("Shutting down DataCacheService...")
        self.close()
        sys.exit(0)

    def close(self):
        """Close database connection and clean up."""
        # Nothing to do here currently
        pass

    def extract_trade_data_from_df(self, df):
        """
        Extract nested trade data fields from DataFrame.

        Args:
            df: DataFrame with possibly nested trade data

        Returns:
            DataFrame with flattened trade data fields
        """
        result_df = df.copy()

        # Check if tradeLast5Seconds or tradeLast10Seconds are in columns
        trade_columns = [col for col in df.columns if col in ["tradeLast5Seconds", "tradeLast10Seconds"]]

        if not trade_columns:
            return result_df

        # Function to safely get value from nested dict
        def get_nested(d, path, default=None):
            if not isinstance(d, dict):
                return default
            parts = path.split(".")
            current = d
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return default
            return current

        # Extract trade data from 5s and 10s
        for period in [5, 10]:
            trade_col = f"tradeLast{period}Seconds"

            if trade_col not in df.columns:
                continue

            # For each row, extract trade data
            for i, row in df.iterrows():
                trade_data = row.get(trade_col)
                if not isinstance(trade_data, dict):
                    continue

                # Extract volume fields
                for vol_type in ["buy", "sell", "bot"]:
                    col_name = f"trade_last{period}Seconds_volume_{vol_type}"
                    result_df.at[i, col_name] = get_nested(trade_data, f"volume.{vol_type}")

                # Extract tradeCount fields
                for side in ["buy", "sell"]:
                    for size in ["small", "medium", "large", "big", "super"]:
                        col_name = f"trade_last{period}Seconds_tradeCount_{side}_{size}"
                        result_df.at[i, col_name] = get_nested(trade_data, f"tradeCount.{side}.{size}")

                # Extract bot trade count
                col_name = f"trade_last{period}Seconds_tradeCount_bot"
                result_df.at[i, col_name] = get_nested(trade_data, "tradeCount.bot")

        # Remove the original nested columns
        for col in trade_columns:
            if col in result_df.columns:
                del result_df[col]

        return result_df

    def get_verified_pools(self) -> List[Dict[str, Any]]:
        """
        Get list of verified pools from the cache.
        
        Returns:
            List of dictionaries containing pool_id, verified_at, note, and data_points
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.execute(
                """
                SELECT pool_id, verified_at, note, data_points
                FROM verified_pools
                ORDER BY verified_at DESC
                """
            )
            
            # Convert to list of dictionaries
            result = []
            for row in cursor.fetchall():
                result.append({
                    "pool_id": row[0],
                    "poolAddress": row[0],  # Additional alias for compatibility
                    "verified_at": row[1],
                    "note": row[2],
                    "data_points": row[3] if len(row) > 3 else 0  # Handle older rows without data_points
                })
            
            return result
        
        except Exception as e:
            logger.error(f"Error getting verified pools: {e}")
            return []
        finally:
            if "conn" in locals():
                conn.close()

    def mark_pools_verified(self, pool_ids: List[str], note: str = "", data_points: Dict[str, int] = None) -> int:
        """
        Mark pools as verified in the database.
        
        Args:
            pool_ids: List of pool IDs to mark as verified
            note: Optional note about the verification
            data_points: Optional dictionary mapping pool_ids to their data point counts
        
        Returns:
            Number of pools successfully marked
        """
        if not pool_ids:
            return 0
        
        if data_points is None:
            data_points = {}
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Begin transaction
            conn.execute("BEGIN TRANSACTION")
            
            marked_count = 0
            current_time = datetime.now().isoformat()
            
            for pool_id in pool_ids:
                try:
                    # Get data_points for this pool if available
                    points = data_points.get(pool_id.lower(), 0)
                    
                    # Insert or replace entry
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO verified_pools
                        (pool_id, verified_at, note, data_points)
                        VALUES (?, ?, ?, ?)
                        """,
                        (pool_id, current_time, note, points)
                    )
                    marked_count += 1
                except Exception as e:
                    logger.error(f"Error marking pool {pool_id} as verified: {e}")
            
            # Commit transaction
            conn.commit()
            
            logger.info(f"Marked {marked_count} pools as verified")
            return marked_count
        
        except Exception as e:
            logger.error(f"Error marking pools as verified: {e}")
            if "conn" in locals():
                conn.rollback()
            return 0
        finally:
            if "conn" in locals():
                conn.close()

    def record_pool_data_points(self, pool_data: Dict[str, int], last_checked: str = None) -> int:
        """
        Record data point counts for pools without marking them as verified.
        Used for pools that don't meet minimum data point requirements.
        
        Args:
            pool_data: Dictionary mapping pool_id to data point count
            last_checked: Optional timestamp for when the pools were checked
        
        Returns:
            Number of pools successfully recorded
        """
        if not pool_data:
            return 0
        
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            # Begin transaction
            conn.execute("BEGIN TRANSACTION")
            
            recorded_count = 0
            current_time = last_checked or datetime.now().isoformat()
            
            for pool_id, points in pool_data.items():
                try:
                    # Check if pool already exists in verified_pools
                    cursor = conn.execute(
                        "SELECT pool_id FROM verified_pools WHERE pool_id = ?",
                        (pool_id,)
                    )
                    
                    if cursor.fetchone():
                        # Update only data_points for existing entries
                        conn.execute(
                            """
                            UPDATE verified_pools
                            SET data_points = ?
                            WHERE pool_id = ?
                            """,
                            (points, pool_id)
                        )
                    else:
                        # Insert new entry with "Not verified" note
                        conn.execute(
                            """
                            INSERT INTO verified_pools
                            (pool_id, verified_at, note, data_points)
                            VALUES (?, ?, ?, ?)
                            """,
                            (pool_id, current_time, "Not verified - insufficient data points", points)
                        )
                    
                    recorded_count += 1
                except Exception as e:
                    logger.error(f"Error recording data points for pool {pool_id}: {e}")
            
            # Commit transaction
            conn.commit()
            
            logger.info(f"Recorded data points for {recorded_count} pools")
            return recorded_count
        
        except Exception as e:
            logger.error(f"Error recording pool data points: {e}")
            if "conn" in locals():
                conn.rollback()
            return 0
        finally:
            if "conn" in locals():
                conn.close()

    def get_pools_with_datapoints_below_threshold(self, threshold: int = 600) -> List[Dict[str, Any]]:
        """
        Get pools with recorded data points below the specified threshold.
        
        Args:
            threshold: Data point threshold 
            
        Returns:
            List of pool entries with data points below threshold
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.execute(
                """
                SELECT pool_id, verified_at, note, data_points
                FROM verified_pools
                WHERE data_points > 0 AND data_points < ?
                ORDER BY data_points DESC
                """,
                (threshold,)
            )
            
            # Convert to list of dictionaries
            result = []
            for row in cursor.fetchall():
                result.append({
                    "pool_id": row[0],
                    "poolAddress": row[0],
                    "verified_at": row[1],
                    "note": row[2],
                    "data_points": row[3]
                })
            
            return result
        
        except Exception as e:
            logger.error(f"Error getting pools with low data points: {e}")
            return []
        finally:
            if "conn" in locals():
                conn.close()

    def get_pools_with_datapoints(self, min_data_points: int = 0) -> List[Dict[str, Any]]:
        """
        Get all pools with their data point counts from the cache.
        
        Args:
            min_data_points: Minimum number of data points required
            
        Returns:
            List of dictionaries with pool information and data point counts
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            
            query = """
            SELECT 
                poolAddress, 
                creationTime, 
                lastUpdated, 
                dataPoints, 
                minTimestamp, 
                maxTimestamp, 
                metadata
            FROM pools
            """
            
            params = []
            
            if min_data_points > 0:
                query += " WHERE dataPoints >= ?"
                params.append(min_data_points)
            
            query += " ORDER BY dataPoints DESC"
            
            cursor = conn.execute(query, params)
            columns = [col[0] for col in cursor.description]
            
            result = []
            for row in cursor.fetchall():
                pool_info = {columns[i]: row[i] for i in range(len(columns))}
                result.append(pool_info)
            
            return result
        
        except Exception as e:
            logger.error(f"Error getting pools with datapoints: {e}")
            return []
        finally:
            if "conn" in locals():
                conn.close()
