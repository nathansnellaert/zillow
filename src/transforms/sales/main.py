"""Transform Zillow Sales data by region type."""

from subsets_utils import upload_data, publish
from ..common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, merge_variants, standardize_columns, to_table
)
from .test import test

SALES_VARIANTS = {
    "median_list_price": "median_list_price",
    "median_sale_price": "median_sale_price",
    "sales_count": "sales_count",
    "pct_sold_above_list": "pct_sold_above_list",
    "pct_sold_below_list": "pct_sold_below_list",
    "days_to_pending": "days_to_pending",
    "price_cut_share": "pct_price_cut",
}


def make_metadata(region_type: str, columns: list) -> dict:
    """Generate metadata for a sales dataset."""
    label = REGION_TYPE_LABELS[region_type]
    all_col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "state_code": "Two-letter US state code",
        "median_list_price": "Median list price in USD",
        "median_sale_price": "Median sale price in USD",
        "sales_count": "Estimated number of sales (nowcast)",
        "pct_sold_above_list": "Percent of sales above final list price",
        "pct_sold_below_list": "Percent of sales below final list price",
        "days_to_pending": "Mean days from listing to pending",
        "pct_price_cut": "Percent of listings with a price cut",
    }
    col_descs = {k: v for k, v in all_col_descs.items() if k in columns}

    return {
        "id": f"zillow_sales_{region_type}",
        "title": f"Zillow Sales Metrics by {label}",
        "description": f"Zillow sales and pricing metrics by {label.lower()}. Includes median list/sale prices, sales counts, days to pending, and price cut statistics. Property type is single-family residences and condos.",
        "column_descriptions": col_descs
    }


def run():
    """Transform sales data for all region types."""
    value_cols = list(SALES_VARIANTS.values())

    for region_type in REGION_TYPES:
        print(f"\n  Processing sales {region_type}...")

        dfs = []
        for raw_prefix, col_name in SALES_VARIANTS.items():
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

        dataset_id = f"zillow_sales_{region_type}"
        test(table, region_type)
        upload_data(table, dataset_id)
        publish(dataset_id, make_metadata(region_type, list(merged.columns)))


if __name__ == "__main__":
    run()
