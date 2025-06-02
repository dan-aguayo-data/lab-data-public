import re
import csv
import os

def extract_tables_from_sql(sql_file):
    # Regular expression to match any table name that starts with "DB_ERP"
    table_regex = re.compile(r'DB_ERP\S*', re.IGNORECASE)
    tables = set()

    with open(sql_file, 'r') as file:
        for line in file:
            matches = table_regex.findall(line)
            for match in matches:
                tables.add(match)
        print(f"Found tables in {sql_file}: {tables}")  # Debugging output

    return tables

def write_tables_to_csv(tables_with_files, output_csv):
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Table Name', 'Source SQL'])  # Header for two columns
        for table, source_sql in tables_with_files.items():
            writer.writerow([table, source_sql])  # Write table name and source SQL file

def main():
    sql_folder = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\SQLs'
    output_csv = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\SQLs\tables.csv'
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    tables_with_files = {}  # Dictionary to store table names with corresponding SQL file names

    # Loop through all SQL files in the specified folder
    for filename in os.listdir(sql_folder):
        if filename.endswith('.sql'):
            sql_file_path = os.path.join(sql_folder, filename)
            tables = extract_tables_from_sql(sql_file_path)
            for table in tables:
                tables_with_files[table] = filename  # Map each table to its source file

    write_tables_to_csv(tables_with_files, output_csv)
    print(f"Extracted tables from SQL files. Total unique tables: {len(tables_with_files)}. List saved to {output_csv}")

if __name__ == '__main__':
    main()
