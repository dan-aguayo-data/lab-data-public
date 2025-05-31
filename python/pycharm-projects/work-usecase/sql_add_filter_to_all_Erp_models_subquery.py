import re
import os


def process_ctes_and_row_number(sql_file_path):
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()

    print("\n🔍 Debugging: First 500 characters of the SQL file:\n", sql_content[:500])

    # **STEP 1: Detect subqueries with ROW_NUMBER()**
    row_number_pattern = re.compile(
        r"(\bFROM\b\s*\(\s*SELECT\s+\*,\s+ROW_NUMBER\(\) OVER \(PARTITION BY\s+(\w+)\s+ORDER BY\s+(\w+)\s+DESC\)\s+AS\s+rn\s+FROM\s+(\{\{source\('.*?','.*?'\)\}\})\s*(?:\w*)?\))",
        re.IGNORECASE | re.DOTALL
    )

    row_number_matches = list(row_number_pattern.finditer(sql_content))

    if not row_number_matches:
        print("\n✅ No `ROW_NUMBER()` subqueries found. No changes needed.")
        return

    print("\n✅ **Found `ROW_NUMBER()` Subqueries:**\n")

    for match in row_number_matches:
        full_match = match.group(1)
        partition_col = match.group(2)
        order_col = match.group(3)
        source_block = match.group(4)
        start_pos = match.start()

        print(f"   🔹 Found ROW_NUMBER() with `PARTITION BY {partition_col}` and `ORDER BY {order_col} DESC`")
        print(f"   📍 Located at character position `{start_pos}`\n")

        # ✅ **Check if `_FIVETRAN_DELETED` exists inside the subquery**
        subquery_text = full_match
        has_fivetran = bool(re.search(r"\b_FIVETRAN_DELETED\b", subquery_text, re.IGNORECASE))

        if has_fivetran:
            print(f"   🚀 `_FIVETRAN_DELETED` already exists inside this subquery. Skipping modification.\n")
            continue

        print(f"   ❌ `_FIVETRAN_DELETED` is missing. Updating subquery...\n")

        # ✅ **Insert `_FIVETRAN_DELETED = FALSE` before `WHERE rn = 1`**
        modified_subquery = re.sub(
            r"(\bFROM\s+\{\{source\(.*?\)\}\}(\s*\w*)?)",
            r"\1 WHERE _FIVETRAN_DELETED = FALSE",
            subquery_text,
            count=1  # Only modify the first occurrence
        )

        sql_content = sql_content.replace(full_match, modified_subquery)

    # ✅ **Write updated SQL back to file**
    with open(sql_file_path, 'w', encoding='utf-8') as file:
        file.write(sql_content)

    print("\n✅ **FIVETRAN filter added where necessary!**")
    print(f"\n📌 Updated file: {sql_file_path}")


# Example usage
sql_file_path = r"C:\Users\DanielAguayo\dbt_common\dataplatforms-common-1\dataplatforms-common\models\ERP_STAGE_DAILY\ERP_STAGE_AP_TRANSACTIONS.sql"
process_ctes_and_row_number(sql_file_path)
