# Field Constants and Schema

This directory contains constants and schema definitions for the Solana Trading Simulator project.

## Field Constants

The `fields.py` file defines constants for all field names used in the Firebase database and SQLite cache. This ensures consistent field naming across the codebase and helps prevent typos.

Fields are organized into categories:
- **Timestamp Fields**: Time-related fields (e.g., `timestamp`, `timeFromStart`)
- **Numeric Fields**: Float values (e.g., `marketCap`, `currentPrice`)
- **Integer Fields**: Whole number values (e.g., `holdersCount`, `holderDelta5s`)
- **String Fields**: Text values (e.g., `poolAddress`)
- **Complex Fields**: Fields with nested structures that require serialization (e.g., `tradeLast5Seconds`)

## SQLite Schema

The `schema.sql` file in the `src/data` directory defines the database structure for the SQLite cache. It includes tables for:

- **Pools**: Basic pool information and metadata
- **Market Data**: Time-series data for each pool
- **Analytics**: Calculated metrics and statistics
- **Cache Metadata**: Information about the cache itself

## Usage

### Importing Field Constants

You can import field constants from the `constants` package:

```python
# Import field groups
from constants import NUMERIC_FIELDS, STRING_FIELDS, REQUIRED_FIELDS

# Import specific fields
from constants import FIELD_POOLADDRESS, FIELD_TIMESTAMP, FIELD_MARKETCAP
```

### Working with the Schema

To use the updated schema in your code:

```python
from pathlib import Path
import sqlite3

# Path to the schema file
schema_path = Path("src/data/schema.sql")

# Create a database connection
conn = sqlite3.connect("cache/pools.db")

# Load and execute the schema
with open(schema_path, "r") as f:
    schema_sql = f.read()
    conn.executescript(schema_sql)
```

## Data Type Handling

When working with data from Firebase:

1. Convert Firebase Timestamp objects to Python datetime objects
2. Convert complex nested objects (dicts, lists) to JSON strings using a custom encoder
3. Ensure numeric fields are converted to proper Python types (int, float)
4. Handle null/None values appropriately

## Example

```python
from constants import FIELD_POOLADDRESS, FIELD_TIMESTAMP, FIELD_MARKETCAP
import pandas as pd

# Use constants in your code
df = pd.DataFrame({
    FIELD_POOLADDRESS: ["pool123"],
    FIELD_TIMESTAMP: [pd.Timestamp.now()],
    FIELD_MARKETCAP: [1000.5]
})
``` 