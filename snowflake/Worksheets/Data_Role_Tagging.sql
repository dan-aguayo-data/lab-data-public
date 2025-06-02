

CREATE TAG department;




ALTER ROLE sales_role SET TAG department = 'Sales';
ALTER ROLE finance_role SET TAG department = 'Finance';



SELECT
    t.object_name AS role_name,
    t.tag_name,
    t.tag_value
FROM
    SNOWFLAKE.ACCOUNT_USAGE.TAGS t
WHERE
    t.object_type = 'ROLE';





