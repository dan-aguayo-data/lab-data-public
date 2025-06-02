import gzip

with gzip.open(r'C:\Users\DanielAguayo\Downloads\FILE_NAME.gz', 'rt') as f:
    content = f.read()
    print(content)