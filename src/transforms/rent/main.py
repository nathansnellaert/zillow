"""Transform Zillow Observed Rent Index (ZORI) data by region type."""

from subsets_utils import upload_data, publish
from ..common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, standardize_columns, to_table
)
from .test import test


def make_metadata(region_type: str) -> dict:
    """Generate metadata for a rent dataset."""
    label = REGION_TYPE_LABELS[region_type]
    col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "rent": "Typical monthly rent in USD",
    }
    if region_type != "state":
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"zillow_rent_{region_type}",
        "title": f"Zillow Observed Rent Index by {label}",
        "description": f"Zillow Observed Rent Index (ZORI) by {label.lower()}. ZORI is a smoothed, seasonally adjusted measure of the typical observed market rate rent. Includes single-family, condo, and multifamily rentals.",
        "column_descriptions": col_descs
    }


def run():
    """Transform rent data for all region types."""
    for region_type in REGION_TYPES:
        print(f"\n  Processing rent {region_type}...")

        raw_name = f"zori_{region_type}"
        df = load_and_melt(raw_name, "rent")

        if df.empty:
            print(f"    No data found for {region_type}, skipping")
            continue

        df = standardize_columns(df, region_type, ["rent"])

        if df.empty:
            print(f"    No data after filtering for {region_type}, skipping")
            continue

        table = to_table(df)
        print(f"    {region_type}: {table.num_rows:,} rows")

        dataset_id = f"zillow_rent_{region_type}"
        test(table, region_type)
        upload_data(table, dataset_id)
        publish(dataset_id, make_metadata(region_type))


if __name__ == "__main__":
    run()
