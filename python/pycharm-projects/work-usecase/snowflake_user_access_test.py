#pip install snowflake-connector-python
import snowflake.connector

ctx = snowflake.connector.connect(
    user='SUPERSET_USER',
    password='xxxxx',
    account='xxxxx.australia-east.azure',
    warehouse='xxxxxx',
    role='xxxxxx'
)

cs = ctx.cursor()
try:
    cs.execute("SELECT CURRENT_VERSION()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()



# Fixing the Warning (Optional)
# If you'd like to resolve the warning about permissions, you can try the following steps:
#
# Navigate to the file location:
#
# Open your Windows File Explorer and go to C:\Users\DanielAguayo\.snowflake.
# Right-click the connections.toml file and select Properties.
#
# Check the file permissions:
#
# Go to the Security tab.
# Ensure that your user account (DanielAguayo) has full control over the file and that the permissions are correctly assigned to the current user (and not other accounts).
# If the permissions look incorrect or too open (for example, if "Everyone" or other accounts have full control), adjust them so only your user account can read and write the file.
# Apply changes and try running the script again to see if the warning disappears.