"""
Field Name Utilities

This module provides utilities for handling field name conventions (camelCase and snake_case)
throughout the data pipeline. It helps maintain consistency when dealing with mixed naming
conventions from various data sources.
"""

import re
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# Comprehensive bidirectional mapping between snake_case and camelCase
# This is based on the REQUIRED_FIELDS from pool_analyzer.py
FIELD_MAPPING = {
    # Market Cap Fields - camelCase to snake_case
    "marketCap": "market_cap",
    "athMarketCap": "ath_market_cap",
    "minMarketCap": "min_market_cap",
    "marketCapChange5s": "market_cap_change_5s",
    "marketCapChange10s": "market_cap_change_10s",
    "marketCapChange30s": "market_cap_change_30s",
    "marketCapChange60s": "market_cap_change_60s",
    "maMarketCap10s": "ma_market_cap_10s",
    "maMarketCap30s": "ma_market_cap_30s",
    "maMarketCap60s": "ma_market_cap_60s",
    # Price Fields - camelCase to snake_case
    "currentPrice": "current_price",
    "lastPrice": "last_price",
    "priceChangePercent": "price_change_percent",
    "priceChangeFromStart": "price_change_from_start",
    # Holder Fields - camelCase to snake_case
    "holdersCount": "holders_count",
    "initialHoldersCount": "initial_holders_count",
    "holdersGrowthFromStart": "holders_growth_from_start",
    "holderDelta5s": "holder_delta_5s",
    "holderDelta10s": "holder_delta_10s",
    "holderDelta30s": "holder_delta_30s",
    "holderDelta60s": "holder_delta_60s",
    # Volume Fields - camelCase to snake_case
    "buyVolume5s": "buy_volume_5s",
    "buyVolume10s": "buy_volume_10s",
    "netVolume5s": "net_volume_5s",
    "netVolume10s": "net_volume_10s",
    "totalVolume": "total_volume",
    # Buy Classification Fields - camelCase to snake_case
    "largeBuy5s": "large_buy_5s",
    "largeBuy10s": "large_buy_10s",
    "bigBuy5s": "big_buy_5s",
    "bigBuy10s": "big_buy_10s",
    "superBuy5s": "super_buy_5s",
    "superBuy10s": "super_buy_10s",
    # Trade Data fields are handled through the nested structure processing
    # Metadata - camelCase to snake_case
    "poolAddress": "pool_id",
    "timeFromStart": "time_from_start",
    "creationTime": "creation_time",
    # Add the reverse mapping (snake_case to camelCase)
    "market_cap": "marketCap",
    "ath_market_cap": "athMarketCap",
    "min_market_cap": "minMarketCap",
    "market_cap_change_5s": "marketCapChange5s",
    "market_cap_change_10s": "marketCapChange10s",
    "market_cap_change_30s": "marketCapChange30s",
    "market_cap_change_60s": "marketCapChange60s",
    "ma_market_cap_10s": "maMarketCap10s",
    "ma_market_cap_30s": "maMarketCap30s",
    "ma_market_cap_60s": "maMarketCap60s",
    "current_price": "currentPrice",
    "last_price": "lastPrice",
    "price_change_percent": "priceChangePercent",
    "price_change_from_start": "priceChangeFromStart",
    "holders_count": "holdersCount",
    "initial_holders_count": "initialHoldersCount",
    "holders_growth_from_start": "holdersGrowthFromStart",
    "holder_delta_5s": "holderDelta5s",
    "holder_delta_10s": "holderDelta10s",
    "holder_delta_30s": "holderDelta30s",
    "holder_delta_60s": "holderDelta60s",
    "buy_volume_5s": "buyVolume5s",
    "buy_volume_10s": "buyVolume10s",
    "net_volume_5s": "netVolume5s",
    "net_volume_10s": "netVolume10s",
    "total_volume": "totalVolume",
    "large_buy_5s": "largeBuy5s",
    "large_buy_10s": "largeBuy10s",
    "big_buy_5s": "bigBuy5s",
    "big_buy_10s": "bigBuy10s",
    "super_buy_5s": "superBuy5s",
    "super_buy_10s": "superBuy10s",
    "pool_id": "poolAddress",
    "time_from_start": "timeFromStart",
    "creation_time": "creationTime",
}

# Field variants to handle common misspellings or inconsistencies
FIELD_VARIANTS = {
    "market_cap": ["marketCap", "marketcap", "market_Cap", "MarketCap"],
    "current_price": ["currentPrice", "CurrentPrice", "current_Price", "currentprice"],
    "pool_id": ["poolAddress", "pool_address", "pooladdress", "PoolAddress"],
    # Add more variants as they are discovered in the actual data
}


def camel_to_snake(name: str) -> str:
    """
    Convert a camelCase string to snake_case.

    Args:
        name: The camelCase string to convert

    Returns:
        The snake_case version of the string
    """
    # Handle empty or None input
    if not name:
        return name

    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def snake_to_camel(name: str) -> str:
    """
    Convert a snake_case string to camelCase.

    Args:
        name: The snake_case string to convert

    Returns:
        The camelCase version of the string
    """
    # Handle empty or None input
    if not name:
        return name

    components = name.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def normalize_field_name(field_name: str, target_convention: str = None) -> str:
    """
    Convert any variant of a field name to its canonical form.

    Args:
        field_name: The field name to normalize
        target_convention: Optional target convention ('snake' or 'camel')
                          If provided, will convert to that convention

    Returns:
        The normalized field name
    """
    # Handle empty input
    if not field_name:
        return field_name

    # Check direct mapping first
    if field_name in FIELD_MAPPING:
        canonical = FIELD_MAPPING[field_name]

        # If target convention specified, ensure result is in that convention
        if target_convention == "snake" and "_" not in canonical:
            return camel_to_snake(canonical)
        elif target_convention == "camel" and "_" in canonical:
            return snake_to_camel(canonical)

        return canonical

    # Check variants
    for canonical, variants in FIELD_VARIANTS.items():
        if field_name in variants:
            # If target convention specified, ensure result is in that convention
            if target_convention == "snake" and "_" not in canonical:
                return camel_to_snake(canonical)
            elif target_convention == "camel" and "_" in canonical:
                return snake_to_camel(canonical)

            return canonical

    # If no mapping found but target convention requested, convert
    if target_convention == "snake" and "_" not in field_name:
        return camel_to_snake(field_name)
    elif target_convention == "camel" and "_" in field_name:
        return snake_to_camel(field_name)

    # Return original if no mapping or conversion needed
    return field_name


def get_field_value(data: Dict[str, Any], field_name: str, default: Any = None) -> Any:
    """
    Try to access field using both naming conventions.

    Args:
        data: Dictionary containing the data
        field_name: Field name to access (in any convention)
        default: Default value to return if field not found

    Returns:
        Field value or default if not found
    """
    # Try direct access first
    if field_name in data:
        return data[field_name]

    # Try normalized name
    normalized = normalize_field_name(field_name)
    if normalized in data and normalized != field_name:
        logger.debug(f"Field {field_name} accessed via normalized name {normalized}")
        return data[normalized]

    # Try alternate convention
    if "_" in field_name:  # It's snake_case, try camelCase
        camel_case = snake_to_camel(field_name)
        if camel_case in data:
            logger.debug(f"Field {field_name} accessed via camelCase {camel_case}")
            return data[camel_case]
    else:  # It's camelCase, try snake_case
        snake_case = camel_to_snake(field_name)
        if snake_case in data:
            logger.debug(f"Field {field_name} accessed via snake_case {snake_case}")
            return data[snake_case]

    # Try any known variant
    for canonical, variants in FIELD_VARIANTS.items():
        if field_name in variants or field_name == canonical:
            # Try all possible names for this field
            all_possible = variants + [canonical]
            for possible in all_possible:
                if possible in data:
                    logger.debug(f"Field {field_name} accessed via variant {possible}")
                    return data[possible]

    # Field not found with any naming convention
    return default


def get_df_field(df, field_name: str, default: Any = None):
    """
    Get a field from DataFrame trying both conventions.

    Args:
        df: Pandas DataFrame
        field_name: Field name to access (in any convention)
        default: Default value to return if field not found

    Returns:
        Series with the field data or default if not found
    """
    # Try direct access
    if field_name in df.columns:
        return df[field_name]

    # Try normalized name
    normalized = normalize_field_name(field_name)
    if normalized in df.columns and normalized != field_name:
        logger.debug(f"DataFrame field {field_name} accessed via normalized name {normalized}")
        return df[normalized]

    # Try alternate convention
    if "_" in field_name:  # It's snake_case, try camelCase
        camel_case = snake_to_camel(field_name)
        if camel_case in df.columns:
            logger.debug(f"DataFrame field {field_name} accessed via camelCase {camel_case}")
            return df[camel_case]
    else:  # It's camelCase, try snake_case
        snake_case = camel_to_snake(field_name)
        if snake_case in df.columns:
            logger.debug(f"DataFrame field {field_name} accessed via snake_case {snake_case}")
            return df[snake_case]

    # Try any known variant
    for canonical, variants in FIELD_VARIANTS.items():
        if field_name in variants or field_name == canonical:
            # Try all possible names for this field
            all_possible = variants + [canonical]
            for possible in all_possible:
                if possible in df.columns:
                    logger.debug(f"DataFrame field {field_name} accessed via variant {possible}")
                    return df[possible]

    # If we still haven't found it, and a default was provided, return that
    if default is not None:
        return default

    # Otherwise raise KeyError
    raise KeyError(f"Field '{field_name}' not found in any naming convention")


def normalize_dataframe_columns(df, target_convention: str = "snake"):
    """
    Normalize all column names in a DataFrame to a consistent naming convention.

    Args:
        df: Pandas DataFrame to normalize
        target_convention: Target naming convention ('snake' or 'camel')

    Returns:
        DataFrame with normalized column names in the specified convention
    """
    # Validate target_convention
    if target_convention not in ["snake", "camel"]:
        logger.warning(f"Invalid target_convention '{target_convention}', defaulting to 'snake'")
        target_convention = "snake"

    # Create a mapping for this specific DataFrame
    rename_map = {}
    for col in df.columns:
        norm_name = normalize_field_name(col, target_convention=target_convention)
        if norm_name != col:
            rename_map[col] = norm_name

    # Apply the renaming
    if rename_map:
        logger.debug(f"Normalizing DataFrame columns to {target_convention} case: {rename_map}")
        df = df.rename(columns=rename_map)

    return df


def has_required_fields(data: Dict[str, Any], required_fields: List[str]) -> tuple:
    """
    Check if data has all required fields in any naming convention.

    Args:
        data: Dictionary containing the data
        required_fields: List of required field names

    Returns:
        Tuple of (success, missing_fields)
    """
    missing = []
    for field in required_fields:
        value = get_field_value(data, field)
        if value is None:
            missing.append(field)

    return len(missing) == 0, missing
