"""Zillow Real Estate Data Connector"""
import os
os.environ['CONNECTOR_NAME'] = 'zillow'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.home_value_index.home_value_index import process_home_value_index
from assets.rent_index.rent_index import process_rent_index
from assets.inventory.inventory import process_inventory
from assets.list_price.list_price import process_list_price
from assets.sales_count.sales_count import process_sales_count

def main():
    validate_environment()
    
    # Process each Zillow dataset
    home_value_data = process_home_value_index()
    rent_index_data = process_rent_index()
    inventory_data = process_inventory()
    list_price_data = process_list_price()
    sales_count_data = process_sales_count()
    
    # Upload all datasets
    upload_data(home_value_data, "zillow_home_value_index")
    upload_data(rent_index_data, "zillow_rent_index")
    upload_data(inventory_data, "zillow_inventory")
    upload_data(list_price_data, "zillow_list_price")
    upload_data(sales_count_data, "zillow_sales_count")


if __name__ == "__main__":
    main()