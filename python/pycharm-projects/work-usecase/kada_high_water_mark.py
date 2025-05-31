from kada_collectors.extractors.snowflake import Extractor

kwargs = {my args} # However you choose to construct your args
hwm_kwrgs = {"start_hwm": "end_hwm": } # The hwm values

ext = Extractor(**kwargs)
ext.run(**hwm_kwrgs)






from kada_collectors.extractors.snowflake import Extractor

# Fill in with your actual Snowflake connection details and configurations
kwargs = {
    "username": "your_username",
    "password": "your_password",
    "account": "your_account",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema",
    "role": "your_role",
    "query": "SELECT * FROM your_table"  # or any other required query/table info
}

# Fill in the high watermark values based on your incremental extraction logic
hwm_kwrgs = {
    "start_hwm": "2023-01-01T00:00:00Z",  # Example start high watermark
    "end_hwm": "2023-12-31T23:59:59Z"     # Example end high watermark
}

# Initialize the extractor and run it
ext = Extractor(**kwargs)
ext.run(**hwm_kwrgs)
