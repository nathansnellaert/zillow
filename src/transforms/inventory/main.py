"""Transform Zillow Inventory data by region type."""

from subsets_utils import upload_data, publish
from ..common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, merge_variants, standardize_columns, to_table
)
from .test import test

INVENTORY_VARIANTS = {
    "inventory_for_sale": "for_sale_inventory",
    "new_listings": "new_listings",
    "new_pending": "new_pending",
}


def make_metadata(region_type: str) -> dict:
    """Generate metadata for an inventory dataset."""
    label = REGION_TYPE_LABELS[region_type]
    col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "for_sale_inventory": "Number of for-sale listings active during the month",
        "new_listings": "Number of new listings during the month",
        "new_pending": "Number of listings that went pending during the month",
    }
    if region_type != "state":
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"zillow_inventory_{region_type}",
        "title": f"Zillow Housing Inventory by {label}",
        "description": f"Zillow housing inventory metrics by {label.lower()}. Includes for-sale inventory count, new listings count, and new pending sales count. Property type is single-family residences and condos.",
        "column_descriptions": col_descs
    }


def run():
    """Transform inventory data for all region types."""
    value_cols = list(INVENTORY_VARIANTS.values())

    for region_type in REGION_TYPES:
        print(f"\n  Processing inventory {region_type}...")

        dfs = []
        for raw_prefix, col_name in INVENTORY_VARIANTS.items():
            raw_name = f"{raw_prefix}_{region_type}"
            df = load_and_melt(raw_name, col_name)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            print(f"    No data found for {region_type}, skipping")
            continue

        merged = merge_variants(dfs, value_cols)
        merged = standardize_columns(merged, region_type, value_cols)

        if merged.empty:
            print(f"    No data after filtering for {region_type}, skipping")
            continue

        table = to_table(merged)
        print(f"    {region_type}: {table.num_rows:,} rows")

        dataset_id = f"zillow_inventory_{region_type}"
        test(table, region_type)
        upload_data(table, dataset_id)
        publish(dataset_id, make_metadata(region_type))


if __name__ == "__main__":
    run()
