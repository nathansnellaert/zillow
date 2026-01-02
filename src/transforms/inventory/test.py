"""Validation for Zillow Inventory datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_inventory_{region_type} output. Raises AssertionError on failure."""
    value_columns = ["for_sale_inventory", "new_listings", "new_pending"]

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

    # Check value ranges for present columns
    for col in value_columns:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 10_000_000, f"{col} values seem too high: max={max(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"Inventory data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"
