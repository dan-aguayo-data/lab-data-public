# Load the key from the file
with open('rsa_key_b64.p8', 'r') as file:   #put .p8 file with the specified naming convention in virtual environment path
    key = file.read()

# Replace newline characters with \n
key = key.replace('\n', '')

# Print or save the formatted key
print(key)
