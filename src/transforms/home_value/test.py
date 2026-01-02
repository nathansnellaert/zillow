"""Validation for Zillow Home Value Index datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_home_value_{region_type} output. Raises AssertionError on failure."""
    value_columns = [
        "all_homes", "single_family", "condo",
        "bed_1", "bed_2", "bed_3", "bed_4", "bed_5_plus",
        "bottom_tier", "top_tier"
    ]

    # Build columns spec dynamically based on what's in the table
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
    }
    # state_code may be null type for state-level data (no parent state)
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"
    for col in value_columns:
        if col in table.column_names:
            columns_spec[col] = "double"

    min_rows = {
        "metro": 10000,
        "state": 1000,
        "county": 50000,
        "city": 100000,
        "zip": 500000,
    }.get(region_type, 1000)

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
                assert max(values) <= 50_000_000, f"{col} values seem too high: max={max(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 1990, f"Data goes back too far: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"
