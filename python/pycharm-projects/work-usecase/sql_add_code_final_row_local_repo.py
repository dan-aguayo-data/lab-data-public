import os


directory = r'C:\Users\DanielAguayo\dbt_raw\dataplatforms-raw\dataplatforms-raw\models\raw\CES_MSCM_ORACLE'  # Update this!


if not os.path.exists(directory):
    print(f"Error: Directory does not exist: {directory}")
    exit(1)

# String to find (for the replacement part)
string_to_find = ''')
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
{% endif %}  

'''

# String to replace with (for the replacement part)
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

# String to append to the end of each file
string_to_append = '''
)
select * from SOURCE_QUALIFY
WHERE (1=1) 
{% if target.name.upper() == "PROD" %} 
    AND NOT (upper(LAST_MODIFIED_BY) LIKE ANY ('%@YOPMAIL.COM', '%COEXSERVICES.INFO', '%TESTINATOR.COM'))
{% endif %}
'''


def process_files(directory, old_string, new_string, append_string):
    modified_count = 0
    append_count = 0

    # Iterate through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):  # Process only SQL files
            file_path = os.path.join(directory, filename)
            try:
                # Read the original content
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()

                # Perform the replacement if the string exists
                updated_content = content
                if old_string in content:
                    updated_content = content.replace(old_string, new_string)
                    modified_count += 1
                    print(f"Replacement performed in: {filename}")
                else:
                    print(f"No replacement needed in: {filename}")

               # Ensure there's a newline at the end of the existing content if it doesn't already have one
               # if not updated_content.endswith('\n'):
               #     updated_content += '\n'

                # Add a blank line and append the new string
                updated_content += '\n' + append_string.strip()  # strip() removes any leading/trailing whitespace from append_string

                # Write the updated content back to file
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(updated_content)

                append_count += 1
                print(f"Appended new content to: {filename}")

            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

    return modified_count, append_count


# Execute the replacement and append

replace_count, append_count = process_files(directory, string_to_find, string_to_replace, string_to_append)
print(f"\nProcess completed.")
print(f"Replacements performed in {replace_count} files.")
print(f"Content appended to {append_count} files.")
