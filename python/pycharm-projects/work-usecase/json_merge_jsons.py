import json
import os

# Paths to the directory containing JSON files and the output file
input_dir = r'/JsonSplit'
output_file = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\merged_output.json'

# Initialize an empty list to store the JSON objects
merged_data = []

# Iterate through all files in the directory
for i in range(1, 442):  # Since you have 441 files
    file_name = f'split_{i}.json'
    file_path = os.path.join(input_dir, file_name)

    with open(file_path, 'r') as file:
        data = json.load(file)
        merged_data.append(data)

# Write the merged data into a single JSON file
with open(output_file, 'w') as outfile:
    json.dump(merged_data, outfile, indent=4)

print(f'Merged JSON data written to {output_file}')
