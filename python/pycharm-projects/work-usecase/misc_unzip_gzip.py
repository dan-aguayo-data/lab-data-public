import gzip

# Open the gzip file
with gzip.open(r'C:\tmp\output\METADATA_20240531132749.csv.gz', 'rt') as f:
    # Read the contents
    file_content = f.read()

# Print the contents
print(file_content)