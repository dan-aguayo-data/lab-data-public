import os
import re

# Directory containing dbt model files
model_directory = r'C:\Users\DanielAguayo\dbt_raw\dataplatforms-raw\dataplatforms-raw\models\raw\CES_MSCM_ORACLE'

# Patterns to be found in the files to be changed to new pattern
source_pattern = re.compile(r'\{\{\s*source\(\s*\'LANDING_ORACLE\',\s*\'AWS_RDS_STAGE_CES_MSCM\'\s*\)\s*\}\}')
table_name_pattern = re.compile(r'METADATA:"table-name"')
metadata_pattern = re.compile(r'N.METADATA:timestamp::string')
operation_pattern = re.compile(r'N.METADATA:operation::string')

#new patterns
new_source = "{{ source('LANDING_ORACLE_TEST', 'MSCM_ORACLE_UAT_MV_TEST') }}"
new_table_pattern = 'TABLE_NAME'
new_metadata = 'N.METADATA_TIMESTAMP'
new_operation = 'N.OPERATION'


# Function to update the content
def update_content(content):
    content = source_pattern.sub(new_source, content)
    content = table_name_pattern.sub(new_table_pattern, content)
    content = metadata_pattern.sub(new_metadata, content)
    content = operation_pattern.sub(new_operation, content)
    return content


# Iterate through each file in the directory
for filename in os.listdir(model_directory):
    if filename.endswith(".sql"):  # Ensure we only process SQL files
        file_path = os.path.join(model_directory, filename)

        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        updated_content = update_content(content)

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(updated_content)

print("All files have been updated successfully.")

