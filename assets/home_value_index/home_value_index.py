"""Fetch and process Zillow Home Value Index data"""
import pyarrow as pa
from utils.http_client import get
from utils.io import load_state, save_state
from datetime import datetime, timezone

def fetch_home_value_index():
    """Fetch Zillow Home Value Index data"""
    url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
    response = get(url)
    response.raise_for_status()
    return response.text

def process_home_value_index():
    """Process Zillow Home Value Index data"""
    import pandas as pd
    from io import StringIO
    
    # Load state
    state = load_state("home_value_index")
    
    # Fetch data
    csv_data = fetch_home_value_index()
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
    save_state("home_value_index", {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "row_count": len(table)
    })
    
    return table