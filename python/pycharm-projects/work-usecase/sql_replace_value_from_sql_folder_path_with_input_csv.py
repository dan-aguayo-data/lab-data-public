import csv
import os

def read_table_mappings(csv_file):
    """ Reads the CSV file and returns a dictionary of table mappings. """
    mapping = {}
    with open(csv_file, mode='r', newline='', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            try:
                mapping[row['Current_Table']] = row['New_Table']
            except KeyError as e:
                print(f"Error with row: {row}")
                print(f"KeyError: {e}")
                continue
    return mapping

def replace_in_file(file_path, mappings):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    for current_table, new_table in mappings.items():
        content = content.replace(current_table, new_table)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

def main():
    sql_folder = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\SQL_Sources'
    csv_file = r'/SQL_Sources/source_replacement.csv'

    table_mappings = read_table_mappings(csv_file)

    for filename in os.listdir(sql_folder):
        if filename.endswith('.sql'):
            file_path = os.path.join(sql_folder, filename)
            replace_in_file(file_path, table_mappings)
            print(f'Processed {filename}')

if __name__ == '__main__':
    main()
