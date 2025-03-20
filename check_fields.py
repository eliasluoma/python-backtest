#!/usr/bin/env python3
"""
Temporary script to check the number of fields in pools after format conversion.
"""

from src.data.firebase_service import FirebaseService


def main():
    """
    Main function to check the fields in the pools.
    """
    # Initialize the Firebase service
    firebase_service = FirebaseService()

    # Pool IDs to check
    pool_ids = [
        "2vpAeyJCX7Wi93cXLuSaZYZb78JGSCjYML345jW3DUN2",  # Pool 1 format
        "12H7zN3gXRfUeu2fCQjooPAofbBL2wz7X7wKoS44oJkX",  # Pool 2 format
    ]

    for pool_id in pool_ids:
        print(f"\n{'='*80}")
        print(f"Checking pool: {pool_id}")
        print(f"{'='*80}")

        # Fetch the pool data
        df = firebase_service.fetch_pool_data(pool_id)

        if df.empty:
            print(f"No data found for pool {pool_id}")
            continue

        # Print the number of data points and regular fields
        print(f"Data points: {len(df)}")
        print(f"Field count (top level): {len(df.columns)}")

        # Get the first row as-is
        first_row = df.iloc[0].to_dict()

        # Get the first row processed with FirebaseService's new methods
        processed_row = firebase_service.prepare_for_database(first_row)

        print(f"Field count after flattening: {len(processed_row)}")

        # Print all column names
        print("\nTop-level columns (before flattening):")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. {col}")

        # Print all flattened fields
        print("\nAll fields after flattening (same format as will be stored in database):")
        for i, (key, value) in enumerate(sorted(processed_row.items())):
            value_type = type(value).__name__
            value_str = str(value)
            if len(value_str) > 40:  # Truncate long values
                value_str = value_str[:37] + "..."
            print(f"  {i+1}. {key} ({value_type}): {value_str}")

        # Test with first 2 rows
        if len(df) >= 2:
            print("\nTest processing with first 2 rows:")
            test_df = df.head(2)
            processed_df = firebase_service.preprocess_data(test_df)
            print(f"Original DataFrame shape: {test_df.shape}")
            print(f"Processed DataFrame shape: {processed_df.shape}")
            print(f"Processed columns count: {len(processed_df.columns)}")

            # Print a few column names for verification
            print("\nSample columns from processed DataFrame:")
            for col in list(processed_df.columns)[:10]:
                print(f"  - {col}")


if __name__ == "__main__":
    main()
