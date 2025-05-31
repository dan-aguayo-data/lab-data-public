import gzip

with gzip.open(r'C:\Users\DanielAguayo\Downloads\LABOUR_ACTUALS_20241212182633.gz', 'rt') as f:
    content = f.read()
    print(content)