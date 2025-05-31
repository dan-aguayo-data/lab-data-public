import os
import glob

# Define the root directory
root_dir = r"C:\Users\DanielAguayo\lab-data-dan\snowflake\Worksheets"

# Recursively find all .txt files
txt_files = glob.glob(os.path.join(root_dir, "**", "*.txt"), recursive=True)

for txt_file in txt_files:
    # Create new filename with .sql extension
    sql_file = os.path.splitext(txt_file)[0] + ".sql"

    # Read the contents of the .txt file
    with open(txt_file, "r", encoding="utf-8") as infile:
        content = infile.read()

    # Write the contents to the .sql file
    with open(sql_file, "w", encoding="utf-8") as outfile:
        outfile.write(content)