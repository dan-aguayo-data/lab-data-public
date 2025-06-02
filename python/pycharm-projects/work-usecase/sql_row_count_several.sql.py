import os
import re

directory = r'C:\Users\DanielAguayo\path'

for filename in os.listdir(directory):
    if filename.endswith(".sql"):  # Process only SQL files
        file_path = os.path.join(directory, filename)
        # Remove .sql extension by getting everything before the last 4 characters
        new_filename = filename[7:-4]


    with open(file_path, 'r', encoding='utf-8') as file:

        content = file.read()
        pattern = r'(?i)\{%\s*if\s*target\.name\.upper\(\)\s*==\s*"PROD"\s*%}\s*(.*?)\s*\{%\s*endif\s*%}'
        match = re.search(pattern, content, re.DOTALL)
        extracted_text = match.group(1).strip()
    print(f"SELECT '{new_filename}' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM {new_filename} \n{extracted_text} \n AND CAST(CREATED_ON AS DATE) < DATE '2025-03-05'\nUNION ALL")