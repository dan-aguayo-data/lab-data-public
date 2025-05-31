import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create a sample DataFrame
data = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, 35, 40, 45],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
}

df = pd.DataFrame(data)

# Define the schema
schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('age', pa.int32()),
    ('city', pa.string())
])

# Convert DataFrame to PyArrow Table
table = pa.Table.from_pandas(df, schema=schema)

# Write the table to a Parquet file
pq.write_table(table, 'sample_data.parquet')

print("Parquet file 'sample_data.parquet' has been created.")
