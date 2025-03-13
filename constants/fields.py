"""
Field constants for Solana Trading Simulator.

This file defines the field names used in the Firebase database and SQLite cache.
These constants help maintain consistency across the codebase.
"""

# Timestamp Fields
# These fields store time-related data (datetime objects, timestamps)
FIELD_TIMESTAMP = "timestamp"

# Numeric Fields
# These fields store decimal values (prices, volumes, rates)
FIELD_TIMEFROMSTART = "timeFromStart"
FIELD_ATHMARKETCAP = "athMarketCap"
FIELD_BUYVOLUME10S = "buyVolume10s"
FIELD_BUYVOLUME5S = "buyVolume5s"
FIELD_MAMARKETCAP10S = "maMarketCap10s"
FIELD_MAMARKETCAP30S = "maMarketCap30s"
FIELD_MAMARKETCAP60S = "maMarketCap60s"
FIELD_MARKETCAP = "marketCap"
FIELD_MARKETCAPCHANGE10S = "marketCapChange10s"
FIELD_MARKETCAPCHANGE30S = "marketCapChange30s"
FIELD_MARKETCAPCHANGE5S = "marketCapChange5s"
FIELD_MARKETCAPCHANGE60S = "marketCapChange60s"
FIELD_MINMARKETCAP = "minMarketCap"
FIELD_NETVOLUME10S = "netVolume10s"
FIELD_NETVOLUME5S = "netVolume5s"
FIELD_PRICECHANGEFROMSTART = "priceChangeFromStart"
FIELD_PRICECHANGEPERCENT = "priceChangePercent"

# Integer Fields
# These fields store whole number values (counts, indices)
FIELD_BIGBUY10S = "bigBuy10s"
FIELD_BIGBUY5S = "bigBuy5s"
FIELD_HOLDERDELTA10S = "holderDelta10s"
FIELD_HOLDERDELTA30S = "holderDelta30s"
FIELD_HOLDERDELTA5S = "holderDelta5s"
FIELD_HOLDERDELTA60S = "holderDelta60s"
FIELD_HOLDERSCOUNT = "holdersCount"
FIELD_HOLDERSGROWTHFROMSTART = "holdersGrowthFromStart"
FIELD_INITIALHOLDERSCOUNT = "initialHoldersCount"
FIELD_LARGEBUY10S = "largeBuy10s"
FIELD_LARGEBUY5S = "largeBuy5s"
FIELD_SUPERBUY10S = "superBuy10s"
FIELD_SUPERBUY5S = "superBuy5s"

# String Fields
# These fields store text data (identifiers, names, addresses)
FIELD_CURRENTPRICE = "currentPrice"
FIELD_POOLADDRESS = "poolAddress"

# Special Fields
# These fields store special data or are used for specific purposes
FIELD_ADDITIONAL_DATA = "additional_data"  # For storing extra fields as JSON in SQLite

# Complex Fields
# These fields store nested data structures (dicts, lists)
FIELD_TRADELAST10SECONDS = "tradeLast10Seconds"  # Requires serialization for SQLite
FIELD_TRADELAST5SECONDS = "tradeLast5Seconds"  # Requires serialization for SQLite

# Field Groups (for easier access)
TIMESTAMP_FIELDS = [
    FIELD_TIMEFROMSTART,
    FIELD_TIMESTAMP,
]

NUMERIC_FIELDS = [
    FIELD_ATHMARKETCAP,
    FIELD_BUYVOLUME10S,
    FIELD_BUYVOLUME5S,
    FIELD_MAMARKETCAP10S,
    FIELD_MAMARKETCAP30S,
    FIELD_MAMARKETCAP60S,
    FIELD_MARKETCAP,
    FIELD_MARKETCAPCHANGE10S,
    FIELD_MARKETCAPCHANGE30S,
    FIELD_MARKETCAPCHANGE5S,
    FIELD_MARKETCAPCHANGE60S,
    FIELD_MINMARKETCAP,
    FIELD_NETVOLUME10S,
    FIELD_NETVOLUME5S,
    FIELD_PRICECHANGEFROMSTART,
    FIELD_PRICECHANGEPERCENT,
]

INTEGER_FIELDS = [
    FIELD_BIGBUY10S,
    FIELD_BIGBUY5S,
    FIELD_HOLDERDELTA10S,
    FIELD_HOLDERDELTA30S,
    FIELD_HOLDERDELTA5S,
    FIELD_HOLDERDELTA60S,
    FIELD_HOLDERSCOUNT,
    FIELD_HOLDERSGROWTHFROMSTART,
    FIELD_INITIALHOLDERSCOUNT,
    FIELD_LARGEBUY10S,
    FIELD_LARGEBUY5S,
    FIELD_SUPERBUY10S,
    FIELD_SUPERBUY5S,
]

STRING_FIELDS = [
    FIELD_CURRENTPRICE,
    FIELD_POOLADDRESS,
]

COMPLEX_FIELDS = [
    FIELD_TRADELAST10SECONDS,
    FIELD_TRADELAST5SECONDS,
]

# Special fields for SQLite storage
SPECIAL_FIELDS = [
    FIELD_ADDITIONAL_DATA,
]

# All fields combined
ALL_FIELDS = TIMESTAMP_FIELDS + NUMERIC_FIELDS + INTEGER_FIELDS + STRING_FIELDS + COMPLEX_FIELDS + SPECIAL_FIELDS

# Required fields for basic functionality
REQUIRED_FIELDS = [
    FIELD_POOLADDRESS,
    FIELD_TIMESTAMP,
    FIELD_CURRENTPRICE,
    FIELD_MARKETCAP,
    FIELD_HOLDERSCOUNT,
]
