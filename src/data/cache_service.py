"""
SQLite-based Cache Service for Pool Data

This module provides a caching mechanism for pool data using SQLite as the backend.
It is designed to work with both Python and TypeScript applications.
"""

import sqlite3
import json
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Union, Optional, Any, Tuple

import pandas as pd
from functools import lru_cache

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataCacheService:
    """SQLite-based cache service for pool data"""

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
                conn.execute("INSERT INTO schema_version (id, version, updated_at) VALUES (1, 1, datetime('now'))")

            logger.info(f"Connected to database (schema version: {version[0] if version else 1})")

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
        # Prepare query
        query = """
        SELECT 
            pool_id, timestamp, market_cap, current_price, last_price, 
            ath_market_cap, min_market_cap, ma_market_cap_10s, ma_market_cap_30s, ma_market_cap_60s,
            market_cap_change_5s, market_cap_change_10s, market_cap_change_30s, market_cap_change_60s,
            price_change_percent, price_change_from_start,
            holders_count, initial_holders_count, holders_growth_from_start,
            holder_delta_5s, holder_delta_10s, holder_delta_30s, holder_delta_60s,
            buy_volume_5s, buy_volume_10s, net_volume_5s, net_volume_10s, total_volume,
            large_buy_5s, large_buy_10s, big_buy_5s, big_buy_10s, super_buy_5s, super_buy_10s,
            time_from_start, trade_data, additional_data
        FROM market_data
        WHERE pool_id = ?
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

            # Process JSON columns
            if not df.empty:
                # Parse JSON columns
                if "trade_data" in df.columns:
                    trade_dfs = []
                    for i, row in df.iterrows():
                        if row["trade_data"] and row["trade_data"] != "{}":
                            try:
                                trade_data = json.loads(row["trade_data"])
                                # Flatten trade data columns
                                flat_trade = self._flatten_json(trade_data, parent_key="trade_")
                                trade_dfs.append(pd.Series(flat_trade, name=i))
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in trade_data for pool {pool_id}")
                                trade_dfs.append(pd.Series({}, name=i))
                        else:
                            trade_dfs.append(pd.Series({}, name=i))

                    if trade_dfs:
                        trade_df = pd.DataFrame(trade_dfs)
                        if not trade_df.empty:
                            df = pd.concat([df.drop("trade_data", axis=1), trade_df], axis=1)

                # Parse additional_data
                if "additional_data" in df.columns:
                    add_dfs = []
                    for i, row in df.iterrows():
                        if row["additional_data"] and row["additional_data"] != "{}":
                            try:
                                add_data = json.loads(row["additional_data"])
                                add_dfs.append(pd.Series(add_data, name=i))
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in additional_data for pool {pool_id}")
                                add_dfs.append(pd.Series({}, name=i))
                        else:
                            add_dfs.append(pd.Series({}, name=i))

                    if add_dfs:
                        add_df = pd.DataFrame(add_dfs)
                        if not add_df.empty:
                            df = pd.concat([df.drop("additional_data", axis=1), add_df], axis=1)

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

    def _flatten_json(self, json_obj: Dict, parent_key: str = "") -> Dict:
        """Flatten nested JSON objects for easier DataFrame conversion"""
        items = {}
        for k, v in json_obj.items():
            new_key = f"{parent_key}{k}" if parent_key else k

            if isinstance(v, dict):
                items.update(self._flatten_json(v, f"{new_key}."))
            else:
                items[new_key] = v
        return items

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

        # Standardize column names to snake_case
        df = self._standardize_column_names(df)

        # Ensure timestamp column exists and is in datetime format
        if "timestamp" not in df.columns:
            logger.error(f"No timestamp column in DataFrame for pool {pool_id}")
            return False

        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Ensure pool_id column exists
        if "pool_id" not in df.columns:
            df["pool_id"] = pool_id

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Extract metadata
        min_timestamp = df["timestamp"].min()
        max_timestamp = df["timestamp"].max()
        data_points = len(df)

        try:
            # Connect to database
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA foreign_keys = ON")

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            try:
                # Check if pool exists
                cursor = conn.execute("SELECT pool_id FROM pools WHERE pool_id = ?", (pool_id,))
                pool_exists = cursor.fetchone() is not None

                # Handle pool metadata
                if pool_exists:
                    if replace:
                        # Update with replacement
                        conn.execute(
                            """
                        UPDATE pools SET 
                            last_updated = datetime('now'),
                            data_points = ?,
                            min_timestamp = ?,
                            max_timestamp = ?
                        WHERE pool_id = ?
                        """,
                            (data_points, min_timestamp.isoformat(), max_timestamp.isoformat(), pool_id),
                        )
                    else:
                        # Update with append
                        conn.execute(
                            """
                        UPDATE pools SET 
                            last_updated = datetime('now'),
                            data_points = data_points + ?,
                            min_timestamp = MIN(min_timestamp, ?),
                            max_timestamp = MAX(max_timestamp, ?)
                        WHERE pool_id = ?
                        """,
                            (data_points, min_timestamp.isoformat(), max_timestamp.isoformat(), pool_id),
                        )
                else:
                    # Insert new pool
                    conn.execute(
                        """
                    INSERT INTO pools (
                        pool_id, creation_time, last_updated, 
                        data_points, min_timestamp, max_timestamp, metadata
                    ) VALUES (?, datetime('now'), datetime('now'), ?, ?, ?, ?)
                    """,
                        (pool_id, data_points, min_timestamp.isoformat(), max_timestamp.isoformat(), "{}"),
                    )

                # Handle market data
                if replace:
                    # Delete existing data
                    conn.execute("DELETE FROM market_data WHERE pool_id = ?", (pool_id,))

                # Prepare data for insertion
                records = []
                for _, row in df.iterrows():
                    # Extract special columns for JSON storage
                    trade_data: Dict[str, Any] = {}
                    additional_data: Dict[str, Any] = {}

                    for col in row.index:
                        # Check if column is trade-related or other
                        if col.startswith("trade_"):
                            # Add to trade_data
                            parts = col.split(".")
                            curr = trade_data
                            for i, part in enumerate(parts[:-1]):
                                if part not in curr:
                                    curr[part] = {}
                                curr = curr[part]
                            curr[parts[-1]] = row[col]
                        elif col not in [
                            "pool_id",
                            "timestamp",
                            "market_cap",
                            "current_price",
                            "last_price",
                            "ath_market_cap",
                            "min_market_cap",
                            "ma_market_cap_10s",
                            "ma_market_cap_30s",
                            "ma_market_cap_60s",
                            "market_cap_change_5s",
                            "market_cap_change_10s",
                            "market_cap_change_30s",
                            "market_cap_change_60s",
                            "price_change_percent",
                            "price_change_from_start",
                            "holders_count",
                            "initial_holders_count",
                            "holders_growth_from_start",
                            "holder_delta_5s",
                            "holder_delta_10s",
                            "holder_delta_30s",
                            "holder_delta_60s",
                            "buy_volume_5s",
                            "buy_volume_10s",
                            "net_volume_5s",
                            "net_volume_10s",
                            "total_volume",
                            "large_buy_5s",
                            "large_buy_10s",
                            "big_buy_5s",
                            "big_buy_10s",
                            "super_buy_5s",
                            "super_buy_10s",
                            "time_from_start",
                        ]:
                            # Add to additional_data
                            additional_data[col] = row[col]

                    # Convert to JSON strings
                    trade_json = json.dumps(trade_data) if trade_data else "{}"
                    additional_json = json.dumps(additional_data) if additional_data else "{}"

                    # Create record for insertion
                    record = (
                        row.get("pool_id"),
                        row.get("timestamp").isoformat(),
                        row.get("market_cap"),
                        row.get("current_price"),
                        row.get("last_price"),
                        row.get("ath_market_cap"),
                        row.get("min_market_cap"),
                        row.get("ma_market_cap_10s"),
                        row.get("ma_market_cap_30s"),
                        row.get("ma_market_cap_60s"),
                        row.get("market_cap_change_5s"),
                        row.get("market_cap_change_10s"),
                        row.get("market_cap_change_30s"),
                        row.get("market_cap_change_60s"),
                        row.get("price_change_percent"),
                        row.get("price_change_from_start"),
                        row.get("holders_count"),
                        row.get("initial_holders_count"),
                        row.get("holders_growth_from_start"),
                        row.get("holder_delta_5s"),
                        row.get("holder_delta_10s"),
                        row.get("holder_delta_30s"),
                        row.get("holder_delta_60s"),
                        row.get("buy_volume_5s"),
                        row.get("buy_volume_10s"),
                        row.get("net_volume_5s"),
                        row.get("net_volume_10s"),
                        row.get("total_volume"),
                        row.get("large_buy_5s"),
                        row.get("large_buy_10s"),
                        row.get("big_buy_5s"),
                        row.get("big_buy_10s"),
                        row.get("super_buy_5s"),
                        row.get("super_buy_10s"),
                        row.get("time_from_start"),
                        trade_json,
                        additional_json,
                    )
                    records.append(record)

                # Insert data in batches
                batch_size = 100
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]
                    conn.executemany(
                        """
                    INSERT OR REPLACE INTO market_data (
                        pool_id, timestamp, market_cap, current_price, last_price,
                        ath_market_cap, min_market_cap, ma_market_cap_10s, ma_market_cap_30s, ma_market_cap_60s,
                        market_cap_change_5s, market_cap_change_10s, market_cap_change_30s, market_cap_change_60s,
                        price_change_percent, price_change_from_start,
                        holders_count, initial_holders_count, holders_growth_from_start,
                        holder_delta_5s, holder_delta_10s, holder_delta_30s, holder_delta_60s,
                        buy_volume_5s, buy_volume_10s, net_volume_5s, net_volume_10s, total_volume,
                        large_buy_5s, large_buy_10s, big_buy_5s, big_buy_10s, super_buy_5s, super_buy_10s,
                        time_from_start, trade_data, additional_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        batch,
                    )

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
        Standardize column names to snake_case.

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with standardized column names
        """
        # Make a copy of the DataFrame to avoid modifying the original
        result = df.copy()

        # Map of common camelCase to snake_case names
        name_map = {
            "poolAddress": "pool_id",
            "marketCap": "market_cap",
            "currentPrice": "current_price",
            "lastPrice": "last_price",
            "athMarketCap": "ath_market_cap",
            "minMarketCap": "min_market_cap",
            "maMarketCap10s": "ma_market_cap_10s",
            "maMarketCap30s": "ma_market_cap_30s",
            "maMarketCap60s": "ma_market_cap_60s",
            "marketCapChange5s": "market_cap_change_5s",
            "marketCapChange10s": "market_cap_change_10s",
            "marketCapChange30s": "market_cap_change_30s",
            "marketCapChange60s": "market_cap_change_60s",
            "priceChangePercent": "price_change_percent",
            "priceChangeFromStart": "price_change_from_start",
            "holdersCount": "holders_count",
            "initialHoldersCount": "initial_holders_count",
            "holdersGrowthFromStart": "holders_growth_from_start",
            "holderDelta5s": "holder_delta_5s",
            "holderDelta10s": "holder_delta_10s",
            "holderDelta30s": "holder_delta_30s",
            "holderDelta60s": "holder_delta_60s",
            "buyVolume5s": "buy_volume_5s",
            "buyVolume10s": "buy_volume_10s",
            "netVolume5s": "net_volume_5s",
            "netVolume10s": "net_volume_10s",
            "totalVolume": "total_volume",
            "largeBuy5s": "large_buy_5s",
            "largeBuy10s": "large_buy_10s",
            "bigBuy5s": "big_buy_5s",
            "bigBuy10s": "big_buy_10s",
            "superBuy5s": "super_buy_5s",
            "superBuy10s": "super_buy_10s",
            "timeFromStart": "time_from_start",
        }

        # Rename columns based on mapping
        columns_to_rename = {col: name_map[col] for col in result.columns if col in name_map}
        if columns_to_rename:
            result = result.rename(columns=columns_to_rename)

        return result

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

            query = "SELECT pool_id FROM pools"
            params = []

            if min_data_points > 0:
                query += " WHERE data_points >= ?"
                params.append(min_data_points)

            query += " ORDER BY data_points DESC LIMIT ?"
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
            SELECT p.pool_id
            FROM pools p
            """

            params: List[Union[str, int, float]] = []
            where_clauses = []

            # Add data points filter
            if min_data_points > 0:
                where_clauses.append("p.data_points >= ?")
                params.append(min_data_points)

            # Add market cap filter if needed
            if min_market_cap is not None:
                query += """
                JOIN (
                    SELECT pool_id, MAX(timestamp) as latest_ts
                    FROM market_data
                    GROUP BY pool_id
                ) latest ON p.pool_id = latest.pool_id
                JOIN market_data md ON latest.pool_id = md.pool_id AND latest.latest_ts = md.timestamp
                """
                where_clauses.append("md.market_cap >= ?")
                params.append(float(min_market_cap))

                # Add holders filter if needed
                if min_holders is not None:
                    where_clauses.append("md.holders_count >= ?")
                    params.append(min_holders)
            elif min_holders is not None:
                # Only holders filter, no market cap filter
                query += """
                JOIN (
                    SELECT pool_id, MAX(timestamp) as latest_ts
                    FROM market_data
                    GROUP BY pool_id
                ) latest ON p.pool_id = latest.pool_id
                JOIN market_data md ON latest.pool_id = md.pool_id AND latest.latest_ts = md.timestamp
                """
                where_clauses.append("md.holders_count >= ?")
                params.append(min_holders)

            # Add WHERE clause if any filters
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Add limit
            query += " ORDER BY p.data_points DESC LIMIT ?"
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
            SELECT pool_id, data_points
            FROM pools
            ORDER BY data_points DESC
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
                        data_points = (
                            SELECT COUNT(*) 
                            FROM market_data 
                            WHERE market_data.pool_id = pools.pool_id
                        ),
                        min_timestamp = (
                            SELECT MIN(timestamp) 
                            FROM market_data 
                            WHERE market_data.pool_id = pools.pool_id
                        ),
                        max_timestamp = (
                            SELECT MAX(timestamp) 
                            FROM market_data 
                            WHERE market_data.pool_id = pools.pool_id
                        )
                    """
                    )

                    # Remove pools with no data
                    conn.execute("DELETE FROM pools WHERE data_points = 0")

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
