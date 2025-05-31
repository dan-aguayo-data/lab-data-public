

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





    SELECT DECRYPT(encrypted_data, 'my_strong_password', 'AES256');


    SELECT TO_BINARY('6E336E6F5574575167375556327266506D334B3974773D3D', 'HEX') AS binary_data;

    
SELECT DECRYPT(TO_BINARY('6E336E6F5574575167375556327266506D334B3974773D3D', 'HEX'), 'StrongPassword123!', 'AES256') AS decrypted_data;


SET encryption_key = 'bQeThWmZq4t7w!z%C*F)J@NcRfUjXn2r';
;
SELECT 
  'HelloWorld!!!' AS original_text,
  ENCRYPT('AES256', 'HelloWorld!!!', $encryption_key) AS encrypted_text;
--AES256
;

sELECT 
   DECRYPT('CBC256', '6E336E6F5574575167375556327266506D334B3974773D3D', $encryption_key)  AS decrypted_text  ;
;

SELECT 
  DECRYPT('AES256', '6E336E6F5574575167375556327266506D334B3974773D3D', $encryption_key) AS decrypted_text;
;

   UNSET encryption_key
