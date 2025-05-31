import json
import os


def split_json(file_path, output_dir, rows_per_file):
    with open(file_path, 'r') as file:
        data = json.load(file)

    items = data['items']
    total_items = len(items)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i in range(0, total_items, rows_per_file):
        split_data = {'items': items[i:i + rows_per_file]}
        output_file = os.path.join(output_dir, f"split_{i // rows_per_file + 1}.json")

        with open(output_file, 'w') as outfile:
            json.dump(split_data, outfile, indent=4)

        print(f"Created: {output_file}")


# Parameters
file_path = r'/test_3.json'
output_dir = r'/JsonSplit'
rows_per_file = 2500  # Adjust this number as needed

# Run the function
split_json(file_path, output_dir, rows_per_file)

