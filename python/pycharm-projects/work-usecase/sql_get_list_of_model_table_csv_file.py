import os
import re
import csv

# Define the folder path
folder_path = r"C:\Users\DanielAguayo\PATH"

# Define the output CSV file in the same folder as the .sql files
output_csv = os.path.join(folder_path, "model_source_tables.csv")

# Define join keywords to search for
join_keywords = [
    "FROM",
    "LEFT JOIN",
    "JOIN",
    "INNER JOIN",
    "CROSS JOIN",
    "RIGHT JOIN"
]


# Function to extract table names from SQL content
def extract_tables(sql_content, model_name):
    results = []
    # Preserve original case for table names (no .upper())

    # Step 1: Extract tables from DBT {{ source() }} syntax
    source_pattern = r"\{\{\s*source\(['\"][^'\"]+['\"], ['\"]([^'\"]+)['\"]\)\s*\}\}"
    source_matches = re.finditer(source_pattern, sql_content, re.IGNORECASE)
    for match in source_matches:
        table_name = match.group(1).strip()
        results.append({"Model Name": model_name, "Source Table": table_name})

    # Step 2: Extract tables after join keywords
    for keyword in join_keywords:
        # Create regex pattern to match the keyword and capture the next word
        # Ignore cases where FROM is followed by '(' or 'TABLE('
        if keyword == "FROM":
            pattern = rf"\b{keyword}\b\s+(?!\(|TABLE\()([^\s(]+)"
        else:
            pattern = rf"\b{keyword}\b\s+([^\s(]+)"

        matches = re.finditer(pattern, sql_content, re.IGNORECASE)
        for match in matches:
            table_name = match.group(1).strip()
            # Clean up table name (remove trailing commas, semicolons, etc.)
            table_name = re.sub(r"[,\.;']", "", table_name)
            # Only add if it looks like a valid table name (alphanumeric with underscores or dots)
            if re.match(r"^[A-Z0-9_.]+$", table_name.upper()):
                # Skip if it's an alias or known function
                if table_name.upper() not in ["AS", "TABLE"]:
                    results.append({"Model Name": model_name, "Source Table": table_name})

    # Remove duplicates while preserving order
    seen = set()
    unique_results = []
    for result in results:
        key = (result["Model Name"], result["Source Table"])
        if key not in seen:
            seen.add(key)
            unique_results.append(result)

    return unique_results


# Initialize list to store all results
all_results = []

# Loop through all .sql files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".sql"):
        # Get the model name (filename without .sql extension)
        model_name = os.path.splitext(filename)[0]

        # Read the SQL file
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                sql_content = file.read()

                # Extract tables
                tables = extract_tables(sql_content, model_name)
                all_results.extend(tables)

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Write results to CSV
try:
    with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Model Name", "Source Table"])
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)
    print(f"CSV file '{output_csv}' created successfully.")
except Exception as e:
    print(f"Error writing CSV file: {e}")