"""Fetch and process Zillow Sales Count data"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state
from datetime import datetime, timezone

def fetch_sales_count():
    """Fetch Zillow Sales Count data"""
    url = "https://files.zillowstatic.com/research/public_csvs/sales_count_now/Metro_sales_count_now_uc_sfrcondo_month.csv"
    response = get(url)
    response.raise_for_status()
    return response.text

def process_sales_count():
    """Process Zillow Sales Count data"""
    import pandas as pd
    from io import StringIO
    
    # Load state
    state = load_state("sales_count")
    
    # Fetch data
    csv_data = fetch_sales_count()
    df = pd.read_csv(StringIO(csv_data))
    
    # Transform from wide to long format
    df = df.melt(
        id_vars=["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"], 
        var_name="date", 
        value_name="value"
    )
    
    # Filter to metro areas only
    df = df[df['RegionType'] == 'msa']
    
    # Clean and rename columns
    df = df.rename(columns={
        'RegionID': 'region_id',
        'SizeRank': 'size_rank',
        'RegionName': 'region_name',
        'StateName': 'state_code'
    })
    df = df.drop(columns=['RegionType'])
    
    # Process dates and values
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])
    
    # Convert to integer for sales count
    df['value'] = df['value'].astype(int)
    
    # Sort by date and region
    df = df.sort_values(['date', 'region_name']).reset_index(drop=True)
    
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    # Update state
    save_state("sales_count", {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "row_count": len(table)
    })
    
    return table