import pandas as pd

# Path to your Excel file
file_path = r"C:\Users\DanielAguayo\lab\python\pycharm-projects\work-usecase\zz-input\AWS common model data validation check.xlsx"
output_csv = r"C:\Users\DanielAguayo\lab\python\pycharm-projects\work-usecase\zz-output\output_sheet_names.csv"
# Get the sheet names
sheet_names = pd.ExcelFile(file_path).sheet_names

print(sheet_names)
# Save the list as a CSV file
df = pd.DataFrame(sheet_names, columns=["Sheet Names"])
df.to_csv(output_csv, index=False, encoding='utf-8')

print(f"Sheet names saved to {output_csv}")