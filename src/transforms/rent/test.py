"""Validation for Zillow Rent Index datasets."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_positive


def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_rent_{region_type} output. Raises AssertionError on failure."""
    min_rows = {
        "metro": 5000,
        "state": 500,
        "county": 10000,
        "city": 50000,
        "zip": 100000,
    }.get(region_type, 500)

    # Build columns spec - state_code may be null type for state-level data
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
        "rent": "double",
    }
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"

    validate(table, {
        "columns": columns_spec,
        "not_null": ["date", "region_id", "region_name", "rent"],
        "unique": ["date", "region_id"],
        "min_rows": min_rows,
    })

    assert_valid_date(table, "date")
    assert_positive(table, "rent", allow_zero=False)

    # Check rent ranges
    rents = [v for v in table.column("rent").to_pylist() if v is not None]
    assert min(rents) >= 100, f"Rents seem too low: min={min(rents)}"
    assert max(rents) <= 200000, f"Rents seem too high: max={max(rents)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"ZORI data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"
