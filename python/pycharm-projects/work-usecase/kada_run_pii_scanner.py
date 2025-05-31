import subprocess

# Define the command and arguments
command = [
    'python', 'pii_scanner.py',
    '-e', r'C:\Users\DanielAguayo\PycharmProjects\KADA\kada_snowflake_extractor_config.json',
    '-f', r'C:\Users\DanielAguayo\PycharmProjects\KADA\K_scan_by_SCHEMA_results.csv',
    '-t', 'snowflake',
    '-s', '1000',
    '-o', r'C:\tmp\scanner_sample'
]

# Run the command
subprocess.run(command)



#python kada_pii_scanner.py -e "C:\Users\DanielAguayo\PycharmProjects\KADA\kada_snowflake_extractor_config.json" -f "C:\Users\DanielAguayo\PycharmProjects\KADA\K_scan_by_SCHEMA_results.csv" -t "snowflake" -s 1000 -o "C:\tmp\scanner_sample"




p