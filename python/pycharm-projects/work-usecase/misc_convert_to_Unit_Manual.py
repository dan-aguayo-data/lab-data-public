key = """-----BEGIN ENCRYPTED PRIVATE KEY-----
XXXXXX
-----END ENCRYPTED PRIVATE KEY-----"""

# Replace newline characters with \n for single-line output
formatted_key = key.replace('\n', '\\n')

# Print or save the formatted key
print(formatted_key)
