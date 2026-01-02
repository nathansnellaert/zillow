"""Zillow Real Estate Data Connector

Fetches and transforms Zillow research data including:
- Home Value Index (ZHVI) - multiple tiers, bedroom counts, property types
- Observed Rent Index (ZORI)
- Inventory metrics - for-sale inventory, new listings, new pending
- Sales metrics - prices, counts, ratios, days to pending

Data is split by geographic region type:
- Metro (MSA)
- State
- County
- City
- ZIP Code
"""

import os
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

import argparse
from subsets_utils import validate_environment
from ingest import zillow_data as ingest_zillow
from transforms.home_value import main as transform_home_value
from transforms.rent import main as transform_rent
from transforms.inventory import main as transform_inventory
from transforms.sales import main as transform_sales


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_zillow.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("\n--- Home Value Index ---")
        transform_home_value.run()
        print("\n--- Rent Index ---")
        transform_rent.run()
        print("\n--- Inventory ---")
        transform_inventory.run()
        print("\n--- Sales ---")
        transform_sales.run()


if __name__ == "__main__":
    main()
