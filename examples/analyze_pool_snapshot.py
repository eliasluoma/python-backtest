#!/usr/bin/env python
"""
Analyze a pool snapshot and compare it with our database structure
"""

import json
import pandas as pd
from pprint import pprint

# The JSON snapshot data provided by the user
snapshot_data = {
    "poolAddress": "2BAz5ADw92a5PsrSm6QHnpZmeWXtHgPBAomRgTWFD8kw",
    "marketCap": "61997.152682669470637",
    "athMarketCap": "66704.49505387558448",
    "minMarketCap": "53664.749669537603325",
    "currentPrice": "0.000061997152682669470637",
    "priceChangePercent": 1.6075022073517238,
    "priceChangeFromStart": "15.324925604464173542",
    "timeFromStart": 24,
    "initialHoldersCount": 423,
    "holdersCount": 545,
    "creationTime": "2025-03-01T12:45:13.000Z",
    "marketCapChange5s": "6.3834581808333013629",
    "marketCapChange10s": "13.351410813748751203",
    "marketCapChange30s": "15.324925604464173543",
    "marketCapChange60s": "15.324925604464173543",
    "maMarketCap10s": "57535.90714817745028",
    "maMarketCap30s": "58596.010952922458574",
    "maMarketCap60s": "58596.010952922458574",
    "holderDelta5s": 14,
    "holderDelta10s": 37,
    "holderDelta30s": 98,
    "holderDelta60s": 98,
    "holdersGrowthFromStart": 122,
    "buyVolume5s": 6.737372807,
    "netVolume5s": -9.317161447,
    "largeBuy5s": 0,
    "bigBuy5s": 0,
    "superBuy5s": 0,
    "buyVolume10s": 25.341408904,
    "netVolume10s": -7.390981978,
    "largeBuy10s": 0,
    "bigBuy10s": 1,
    "superBuy10s": 0,
    "trade_last5Seconds.volume.buy": "6.737372807",
    "trade_last5Seconds.volume.sell": "16.054534254",
    "trade_last5Seconds.volume.bot": "0",
    "trade_last5Seconds.tradeCount.buy.small": 31,
    "trade_last5Seconds.tradeCount.buy.medium": 2,
    "trade_last5Seconds.tradeCount.buy.large": 0,
    "trade_last5Seconds.tradeCount.buy.big": 0,
    "trade_last5Seconds.tradeCount.buy.super": 0,
    "trade_last5Seconds.tradeCount.sell.small": 8,
    "trade_last5Seconds.tradeCount.sell.medium": 1,
    "trade_last5Seconds.tradeCount.sell.large": 0,
    "trade_last5Seconds.tradeCount.sell.big": 1,
    "trade_last5Seconds.tradeCount.sell.super": 0,
    "trade_last5Seconds.tradeCount.bot": 0,
    "trade_last10Seconds.volume.buy": "25.341408904",
    "trade_last10Seconds.volume.sell": "32.732390882",
    "trade_last10Seconds.volume.bot": "0",
    "trade_last10Seconds.tradeCount.buy.small": 64,
    "trade_last10Seconds.tradeCount.buy.medium": 4,
    "trade_last10Seconds.tradeCount.buy.large": 0,
    "trade_last10Seconds.tradeCount.buy.big": 1,
    "trade_last10Seconds.tradeCount.buy.super": 0,
    "trade_last10Seconds.tradeCount.sell.small": 18,
    "trade_last10Seconds.tradeCount.sell.medium": 4,
    "trade_last10Seconds.tradeCount.sell.large": 1,
    "trade_last10Seconds.tradeCount.sell.big": 2,
    "trade_last10Seconds.tradeCount.sell.super": 0,
    "trade_last10Seconds.tradeCount.bot": 0,
}

# Convert string numeric values to float for better analysis
for key, value in snapshot_data.items():
    if isinstance(value, str) and key not in ["poolAddress", "creationTime"]:
        try:
            snapshot_data[key] = float(value)
        except ValueError:
            pass  # Keep as string if not convertible


def analyze_snapshot():
    """Analyze the snapshot data and provide insights"""

    # Group fields by category
    field_categories = {
        "Market Cap": [],
        "Price": [],
        "Holders": [],
        "Volume/Buys": [],
        "Trade Data": [],
        "Metadata": [],
        "Other": [],
    }

    # Categorize fields
    for field, value in snapshot_data.items():
        if field.startswith("trade_"):
            field_categories["Trade Data"].append((field, value))
        elif "marketCap" in field:
            field_categories["Market Cap"].append((field, value))
        elif "holder" in field.lower() or "holderDelta" in field:
            field_categories["Holders"].append((field, value))
        elif "volume" in field.lower() or "buy" in field.lower():
            field_categories["Volume/Buys"].append((field, value))
        elif "price" in field.lower():
            field_categories["Price"].append((field, value))
        elif field in ["poolAddress", "timeFromStart", "creationTime"]:
            field_categories["Metadata"].append((field, value))
        else:
            field_categories["Other"].append((field, value))

    # Print analysis by category
    print("\n" + "=" * 80)
    print("POOL SNAPSHOT ANALYSIS")
    print("=" * 80 + "\n")

    print("This analysis compares a specific pool snapshot with our database structure.\n")

    # Print basic pool information
    print("Basic Pool Information:")
    print(f"Pool Address: {snapshot_data.get('poolAddress')}")
    print(f"Creation Time: {snapshot_data.get('creationTime')}")
    print(f"Time From Start: {snapshot_data.get('timeFromStart')} seconds\n")

    # Detailed category analysis
    for category, fields in field_categories.items():
        if fields:
            print(f"\n{category} Analysis:")
            print("-" * 40)

            if category == "Market Cap":
                print(f"Current Market Cap: ${snapshot_data.get('marketCap', 'N/A'):,.2f}")
                print(f"All-Time High: ${snapshot_data.get('athMarketCap', 'N/A'):,.2f}")
                print(f"Minimum: ${snapshot_data.get('minMarketCap', 'N/A'):,.2f}")

                # Market cap changes
                if "marketCapChange5s" in snapshot_data:
                    print(f"5s Change: {snapshot_data.get('marketCapChange5s', 'N/A'):,.2f}%")
                if "marketCapChange10s" in snapshot_data:
                    print(f"10s Change: {snapshot_data.get('marketCapChange10s', 'N/A'):,.2f}%")
                if "marketCapChange30s" in snapshot_data:
                    print(f"30s Change: {snapshot_data.get('marketCapChange30s', 'N/A'):,.2f}%")
                if "marketCapChange60s" in snapshot_data:
                    print(f"60s Change: {snapshot_data.get('marketCapChange60s', 'N/A'):,.2f}%")

                # Moving averages
                if "maMarketCap10s" in snapshot_data:
                    print(f"10s Moving Average: ${snapshot_data.get('maMarketCap10s', 'N/A'):,.2f}")
                if "maMarketCap30s" in snapshot_data:
                    print(f"30s Moving Average: ${snapshot_data.get('maMarketCap30s', 'N/A'):,.2f}")
                if "maMarketCap60s" in snapshot_data:
                    print(f"60s Moving Average: ${snapshot_data.get('maMarketCap60s', 'N/A'):,.2f}")

            elif category == "Price":
                print(f"Current Price: ${snapshot_data.get('currentPrice', 'N/A')}")
                print(f"Price Change %: {snapshot_data.get('priceChangePercent', 'N/A'):,.2f}%")
                print(f"Price Change From Start: {snapshot_data.get('priceChangeFromStart', 'N/A'):,.2f}%")

            elif category == "Holders":
                print(f"Current Holders: {snapshot_data.get('holdersCount', 'N/A')}")
                print(f"Initial Holders: {snapshot_data.get('initialHoldersCount', 'N/A')}")
                print(f"Growth From Start: +{snapshot_data.get('holdersGrowthFromStart', 'N/A')} holders")

                # Holder deltas
                print(f"5s New Holders: +{snapshot_data.get('holderDelta5s', 'N/A')}")
                print(f"10s New Holders: +{snapshot_data.get('holderDelta10s', 'N/A')}")
                print(f"30s New Holders: +{snapshot_data.get('holderDelta30s', 'N/A')}")
                print(f"60s New Holders: +{snapshot_data.get('holderDelta60s', 'N/A')}")

            elif category == "Volume/Buys":
                if "buyVolume5s" in snapshot_data:
                    print(f"5s Buy Volume: {snapshot_data.get('buyVolume5s', 'N/A'):,.2f}")
                if "netVolume5s" in snapshot_data:
                    print(f"5s Net Volume: {snapshot_data.get('netVolume5s', 'N/A'):,.2f}")
                if "buyVolume10s" in snapshot_data:
                    print(f"10s Buy Volume: {snapshot_data.get('buyVolume10s', 'N/A'):,.2f}")
                if "netVolume10s" in snapshot_data:
                    print(f"10s Net Volume: {snapshot_data.get('netVolume10s', 'N/A'):,.2f}")

                # Special buys
                print(f"Large Buys (5s): {snapshot_data.get('largeBuy5s', 'N/A')}")
                print(f"Big Buys (5s): {snapshot_data.get('bigBuy5s', 'N/A')}")
                print(f"Super Buys (5s): {snapshot_data.get('superBuy5s', 'N/A')}")
                print(f"Large Buys (10s): {snapshot_data.get('largeBuy10s', 'N/A')}")
                print(f"Big Buys (10s): {snapshot_data.get('bigBuy10s', 'N/A')}")
                print(f"Super Buys (10s): {snapshot_data.get('superBuy10s', 'N/A')}")

            elif category == "Trade Data":
                # Summarize 5s trade data
                buy_count_5s = sum(
                    [
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.small", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.medium", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.large", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.big", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.super", 0),
                    ]
                )

                sell_count_5s = sum(
                    [
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.small", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.medium", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.large", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.big", 0),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.super", 0),
                    ]
                )

                # Summarize 10s trade data
                buy_count_10s = sum(
                    [
                        snapshot_data.get("trade_last10Seconds.tradeCount.buy.small", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.buy.medium", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.buy.large", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.buy.big", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.buy.super", 0),
                    ]
                )

                sell_count_10s = sum(
                    [
                        snapshot_data.get("trade_last10Seconds.tradeCount.sell.small", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.sell.medium", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.sell.large", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.sell.big", 0),
                        snapshot_data.get("trade_last10Seconds.tradeCount.sell.super", 0),
                    ]
                )

                print("Last 5 Seconds:")
                print(
                    f"  Buy Trades: {buy_count_5s} (Volume: {snapshot_data.get('trade_last5Seconds.volume.buy', 'N/A')})"
                )
                print(
                    f"  Sell Trades: {sell_count_5s} (Volume: {snapshot_data.get('trade_last5Seconds.volume.sell', 'N/A')})"
                )
                print(f"  Bot Trades: {snapshot_data.get('trade_last5Seconds.tradeCount.bot', 'N/A')}")

                print("\nLast 10 Seconds:")
                print(
                    f"  Buy Trades: {buy_count_10s} (Volume: {snapshot_data.get('trade_last10Seconds.volume.buy', 'N/A')})"
                )
                print(
                    f"  Sell Trades: {sell_count_10s} (Volume: {snapshot_data.get('trade_last10Seconds.volume.sell', 'N/A')})"
                )
                print(f"  Bot Trades: {snapshot_data.get('trade_last10Seconds.tradeCount.bot', 'N/A')}")

                # Detailed breakdown
                print("\nDetailed Trade Breakdown:")
                print(
                    "  5s Buy: Small={}, Medium={}, Large={}, Big={}, Super={}".format(
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.small", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.medium", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.large", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.big", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.buy.super", "N/A"),
                    )
                )
                print(
                    "  5s Sell: Small={}, Medium={}, Large={}, Big={}, Super={}".format(
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.small", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.medium", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.large", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.big", "N/A"),
                        snapshot_data.get("trade_last5Seconds.tradeCount.sell.super", "N/A"),
                    )
                )

    # Overall assessment of the pool's condition
    print("\n" + "=" * 80)
    print("POOL CONDITION ASSESSMENT")
    print("=" * 80)

    market_cap = snapshot_data.get("marketCap", 0)
    price_change = snapshot_data.get("priceChangePercent", 0)
    holders_growth = snapshot_data.get("holdersGrowthFromStart", 0)
    net_volume_10s = snapshot_data.get("netVolume10s", 0)

    # Basic market cap assessment
    if market_cap > 60000:
        print("Market Cap: High ($60k+)")
    elif market_cap > 30000:
        print("Market Cap: Medium ($30k-$60k)")
    else:
        print("Market Cap: Low (<$30k)")

    # Price momentum
    if price_change > 10:
        print("Price Momentum: Very Strong (>10%)")
    elif price_change > 5:
        print("Price Momentum: Strong (5-10%)")
    elif price_change > 0:
        print("Price Momentum: Positive (0-5%)")
    elif price_change > -5:
        print("Price Momentum: Slight Decline (0 to -5%)")
    else:
        print("Price Momentum: Strong Decline (<-5%)")

    # Holder growth
    holder_growth_pct = holders_growth / snapshot_data.get("initialHoldersCount", 1) * 100
    if holder_growth_pct > 50:
        print("Holder Growth: Very Strong (>50%)")
    elif holder_growth_pct > 20:
        print("Holder Growth: Strong (20-50%)")
    elif holder_growth_pct > 10:
        print("Holder Growth: Moderate (10-20%)")
    elif holder_growth_pct > 0:
        print("Holder Growth: Mild (0-10%)")
    else:
        print("Holder Growth: Declining")

    # Buy/Sell balance
    if net_volume_10s > 10:
        print("Trading Balance: Strong Buying Pressure")
    elif net_volume_10s > 0:
        print("Trading Balance: Mild Buying Pressure")
    elif net_volume_10s > -10:
        print("Trading Balance: Mild Selling Pressure")
    else:
        print("Trading Balance: Strong Selling Pressure")

    # Compare with database fields
    print("\n" + "=" * 80)
    print("DATABASE COMPARISON")
    print("=" * 80)

    # Expected fields based on our previous analysis
    expected_fields = {
        "athMarketCap",
        "maMarketCap60s",
        "priceChangeFromStart",
        "timeFromStart",
        "trade_last5Seconds.tradeCount.buy.small",
        "marketCap",
        "holderDelta10s",
        "netVolume5s",
        "bigBuy5s",
        "maMarketCap10s",
        "marketCapChange5s",
        "minMarketCap",
        "holdersCount",
        "priceChangePercent",
        "bigBuy10s",
        "buyVolume5s",
        "doc_id",
        "marketCapChange30s",
        "largeBuy5s",
        "holdersGrowthFromStart",
        "initialHoldersCount",
        "maMarketCap30s",
        "superBuy5s",
        "largeBuy10s",
        "marketCapChange60s",
        "holderDelta5s",
        "poolAddress",
        "superBuy10s",
        "netVolume10s",
        "timestamp",
        "currentPrice",
        "holderDelta30s",
        "holderDelta60s",
        "buyVolume10s",
        "marketCapChange10s",
    }

    # Find missing and additional fields
    snapshot_fields = set(snapshot_data.keys())
    missing_fields = expected_fields - snapshot_fields
    additional_fields = snapshot_fields - expected_fields

    if missing_fields:
        print(f"\nMissing fields in this snapshot (compared to our database):")
        for field in sorted(missing_fields):
            print(f"- {field}")

    if additional_fields:
        print(f"\nAdditional fields in this snapshot (not in our typical data):")
        for field in sorted(additional_fields):
            print(f"- {field}")

    common_fields = expected_fields.intersection(snapshot_fields)
    print(f"\nCommon fields: {len(common_fields)} out of {len(expected_fields)} expected fields")

    match_percentage = len(common_fields) / len(expected_fields) * 100
    print(f"Data structure match: {match_percentage:.1f}%")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    analyze_snapshot()
