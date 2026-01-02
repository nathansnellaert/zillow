"""Validation for Zillow Sales datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_sales_{region_type} output. Raises AssertionError on failure."""
    value_columns = [
        "median_list_price", "median_sale_price", "sales_count",
        "pct_sold_above_list", "pct_sold_below_list",
        "days_to_pending", "pct_price_cut"
    ]

    # Build columns spec dynamically based on what's in the table
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
    }
    # state_code may be null type for state-level data
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"
    for col in value_columns:
        if col in table.column_names:
            columns_spec[col] = "double"

    min_rows = {
        "metro": 5000,
        "state": 500,
        "county": 20000,
        "city": 50000,
        "zip": 100000,
    }.get(region_type, 500)

    validate(table, {
        "columns": columns_spec,
        "not_null": ["date", "region_id", "region_name"],
        "unique": ["date", "region_id"],
        "min_rows": min_rows,
    })

    assert_valid_date(table, "date")

    # Check price ranges
    for col in ["median_list_price", "median_sale_price"]:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 100_000_000, f"{col} values seem too high: max={max(values)}"

    # Check percentage ranges
    for col in ["pct_sold_above_list", "pct_sold_below_list", "pct_price_cut"]:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 100, f"{col} values exceed 100%: max={max(values)}"

    # Check days range
    if "days_to_pending" in table.column_names:
        values = [v for v in table.column("days_to_pending").to_pylist() if v is not None]
        if values:
            assert min(values) >= 0, f"days_to_pending has negative values: min={min(values)}"
            assert max(values) <= 365, f"days_to_pending seems too high: max={max(values)}"

    # Check sales_count
    if "sales_count" in table.column_names:
        values = [v for v in table.column("sales_count").to_pylist() if v is not None]
        if values:
            assert min(values) >= 0, f"sales_count has negative values: min={min(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"Sales data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"
