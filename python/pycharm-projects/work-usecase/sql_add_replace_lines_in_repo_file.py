import os
import re

# Directory containing the files (modify this to your folder path)
directory = r'C:\Users\DanielAguayo\dbt_raw\dataplatforms-raw\dataplatforms-raw\models\raw\CES_MSCM_ORACLE' # Replace with your actual directory path

# String to find
string_to_find_1 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(LAST_MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''

string_to_find_2 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(UPDATED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''

string_to_find_3 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(CREATED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''


string_to_find_4 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(LAST_MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
    AND 
    NOT (upper(CREATED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''


string_to_find_5 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(LAST_MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
    AND 
    NOT (upper(CREATED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
    AND 
    NOT (upper(SUBMITTED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''

string_to_find_6 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(NVL(LAST_MODIFIED_BY,'NA')) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''


string_to_find_7 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}'''

string_to_find_8 = ''')
SELECT
    s.*
FROM
    source s
WHERE (1=1)    
{% if is_incremental() %}
    AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
{% endif %}
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}'''



# String to replace with
string_to_replace = '''), SOURCE_QUALIFY AS ( 

    SELECT
        s.*
    FROM
        source s
    WHERE (1=1)    
    {% if is_incremental() %}
        AND NVL(METADATA_TIMESTAMP, current_date) >= (SELECT dateadd(hour, {{ var('inc_offset') }}, max(METADATA_TIMESTAMP)) FROM {{ this }} )
    {% endif %} 

'''

string_to_append = '''
)
select * from SOURCE_QUALIFY
WHERE (1=1) 
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(LAST_MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}
'''
string_before = ''')
select * from SOURCE_QUALIFY
{% if target.name.upper() == "PROD" %}
WHERE (1=1) '''

string_after = '''{% endif %}'''


def replace_in_files(directory, old_string_1, old_string_2, old_string_3, old_string_4,old_string_5,old_string_6,old_string_7,old_string_8, new_string):
    # Counter for modified files
    file_count = 0
    modified_count = 0
    pattern_count = 0
    append_count = 0
    append_file_count =  0
    extracted_texts = {}
    extracted_text = ""
    is_replaced = False

    # Iterate through each file in the directory
    for filename in os.listdir(directory):
        is_replaced = False
        if filename.endswith(".sql"):  # Process only SQL files (you can modify this condition)
            file_path = os.path.join(directory, filename)

            try:
                # Read the original content
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                file_count += 1
                if "SOURCE_QUALIFY" not in content:
                    pattern = r'(?i)\{%\s*if\s*target\.name\.upper\(\)\s*==\s*"PROD"\s*%}\s*(.*?)\s*\{%\s*endif\s*%}'
                    match = re.search(pattern, content, re.DOTALL)
                    if match:
                        extracted_text = match.group(1).strip()
                        extracted_texts[filename] = extracted_text
                        print(f"\nExtracted from {filename}:")
                        print(f"This is the variable text found':\n{extracted_text}")
                        pattern_count += 1


                    if old_string_1 in content:
                        updated_content = content.replace(old_string_1, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_2 in content:
                        updated_content = content.replace(old_string_2, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_3 in content:
                        updated_content = content.replace(old_string_3, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_4 in content:
                        updated_content = content.replace(old_string_4, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_5 in content:
                        updated_content = content.replace(old_string_5, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_6 in content:
                        updated_content = content.replace(old_string_6, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_7 in content:
                        updated_content = content.replace(old_string_7, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    elif old_string_8 in content:
                        updated_content = content.replace(old_string_8, new_string)

                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        modified_count += 1
                        is_replaced = True
                        print(f"Updated: {filename}")

                    else:
                        print(f"No changes needed in: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

            #
            try:
                if "SOURCE_QUALIFY" not in content:
                    if is_replaced:
                        with open(file_path, 'r', encoding='utf-8') as file:
                            content = file.read()
                        updated_content = content
                        string_to_append = f"{string_before}\n{extracted_text}\n{string_after}"
                        updated_content += '\n' + string_to_append.strip()  # strip() removes any leading/trailing whitespace from append_string

                        # Write the updated content back to file
                        with open(file_path, 'w', encoding='utf-8') as file:
                            file.write(updated_content)

                        append_count += 1
                        print(f"Appended new content to: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

    return file_count,modified_count, pattern_count,append_count

def append_files(directory, append_string):
    append_count = 0
    append_file_count =  0
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):  # Process only SQL files
            file_path = os.path.join(directory, filename)
            try:
                # Read the original content
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                append_file_count += +1

                # Perform the replacement if the string exists
                updated_content = content
                # Add a blank line and append the new string
                updated_content += '\n' + append_string.strip()  # strip() removes any leading/trailing whitespace from append_string

                # Write the updated content back to file
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(updated_content)

                append_count += 1
                print(f"Appended new content to: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

    return  append_count,append_file_count




# Execute the replacement

file_count, modified_count, pattern_count,append_count = replace_in_files(directory, string_to_find_1,string_to_find_2,string_to_find_3,string_to_find_4,string_to_find_5,string_to_find_6,string_to_find_7,string_to_find_8, string_to_replace)
#append,append_file_count = append_files(directory, string_to_append)
print(f"\nReplace Process completed. Modified {modified_count} out of {file_count} files.")
print(f"\nPatterns found were: {pattern_count} out of {file_count} files.")
print(f"\nAppends done were: {append_count} out of {file_count} files.")