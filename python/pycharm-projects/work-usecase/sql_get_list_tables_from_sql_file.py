import re
import csv


def extract_tables_from_sql(sql_file):
    # Regular expression to match any table name that starts with "CES_ERP"
    table_regex = re.compile(r'CES_ERP\S*', re.IGNORECASE)

    tables = set()

    with open(sql_file, 'r') as file:
        for line in file:
            matches = table_regex.findall(line)
            for match in matches:
                tables.add(match)  # Add the full match, including "CES_ERP"
        print(tables)
    return sorted(tables)



def write_tables_to_csv(tables, output_csv):
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Table Name'])
        for table in tables:
            writer.writerow([table])  # Write the full match to the CSV


def main():
    # Full path to the SQL file
    sql_file = r'/SQLs/ERP_STAGE_AP_BALANCES.sql'  # Replace with your file path
    output_csv = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\SQLs\tables.csv'  # Replace with desired output CSV path

    tables = extract_tables_from_sql(sql_file)
    write_tables_to_csv(tables, output_csv)
    print(f"Extracted {len(tables)} tables. List saved to {output_csv}")


if __name__ == '__main__':
    main()
