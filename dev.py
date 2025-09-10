"""Development script for testing Zillow connector"""
import os

# Set environment variables for local development
os.environ['CONNECTOR_NAME'] = 'zillow'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'false'
os.environ['DISABLE_STATE'] = 'false'
os.environ['CATALOG_TYPE'] = 'local'
os.environ['DATA_DIR'] = 'data'

# Run the main connector
from main import main

if __name__ == "__main__":
    main()