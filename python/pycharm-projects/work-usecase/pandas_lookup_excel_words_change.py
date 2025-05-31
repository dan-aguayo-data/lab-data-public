import os
import re
import pandas as pd

# Paths
lookup_file = r'C:\Users\DanielAguayo\PycharmProjects\KADA\lookuplist_PVO.csv'
sql_folder = r'C:\Users\DanielAguayo\PycharmProjects\KADA\SQLs'

# Read the lookup CSV file
lookup_df = pd.read_csv(lookup_file)


# Function to replace words in text based on lookup DataFrame
def replace_words(text, lookup_df):
    # Sort by length of LookUp to avoid partial replacements
    lookup_df = lookup_df.sort_values(by='LookUp', key=lambda col: col.str.len(), ascending=False)
    for _, row in lookup_df.iterrows():
        lookup = row['LookUp']
        replace = row['Replace']
        # Replace words, including quotes if present
        text = re.sub(re.escape(lookup), replace, text)
    return text


# Iterate through all .txt files in the specified directory
for filename in os.listdir(sql_folder):
    if filename.endswith('.txt'):
        file_path = os.path.join(sql_folder, filename)
        with open(file_path, 'r') as file:
            content = file.read()

        # Replace words in the content
        new_content = replace_words(content, lookup_df)

        # Write the new content back to the file
        with open(file_path, 'w') as file:
            file.write(new_content)

print("All SQL files have been processed and updated.")
