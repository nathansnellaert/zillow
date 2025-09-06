"""Fetch and process Zillow List Price data"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state
from datetime import datetime, timezone

def fetch_list_price():
    """Fetch Zillow List Price data"""
    url = "https://files.zillowstatic.com/research/public_csvs/mlp/Metro_mlp_uc_sfrcondo_sm_month.csv"
    response = get(url)
    response.raise_for_status()
    return response.text

def process_list_price():
    """Process Zillow List Price data"""
    import pandas as pd
    from io import StringIO
    
    # Load state
    state = load_state("list_price")
    
    # Fetch data
    csv_data = fetch_list_price()
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
    
    # Sort by date and region
    df = df.sort_values(['date', 'region_name']).reset_index(drop=True)
    
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    # Update state
    save_state("list_price", {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "row_count": len(table)
    })
    
    return table