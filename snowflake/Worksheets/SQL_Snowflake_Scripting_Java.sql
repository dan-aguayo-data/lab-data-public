USE DATABASE RAW_DEV ;

USE SCHEMA TEST;

DECLARE view_name STRING;
DECLARE row_count INT;

CREATE OR REPLACE PROCEDURE check_views_data_coex()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = [];
var schema_name = 'COEX_PROD.CES_MSCM';  // Include the database name
var query = `
    SELECT table_name 
    FROM COEX_PROD.information_schema.views  -- Explicitly use COEX_PROD
    WHERE table_schema = 'CES_MSCM'
`;

var stmt = snowflake.createStatement({ sqlText: query });
var views = stmt.execute();

while (views.next()) {
    var view_name = views.getColumnValue(1);
    var count_query = `SELECT COUNT(*) FROM ${schema_name}."${view_name}"`;  // Use double quotes for view names
    try {
        var count_stmt = snowflake.createStatement({ sqlText: count_query });
        var count_result = count_stmt.execute();
        count_result.next();
        var row_count = count_result.getColumnValue(1);

        if (row_count == 0) {
            result.push(view_name + ' has no data.');
        } else {
            result.push(view_name + ' has data.');
        }
    } catch (e) {
        result.push(view_name + ' encountered an error: ' + e.message);
    }
}

return result.join('\n');
$$;



CALL check_views_data_coex();



CREATE OR REPLACE PROCEDURE check_views_data_warrrl()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = [];
var schema_name = 'WARRRL_PROD.CES_MSCM';  // Include the database name
var query = `
    SELECT table_name 
    FROM WARRRL_PROD.information_schema.views  -- Explicitly use WARRRL_PROD
    WHERE table_schema = 'CES_MSCM'
`;

var stmt = snowflake.createStatement({ sqlText: query });
var views = stmt.execute();

while (views.next()) {
    var view_name = views.getColumnValue(1);
    var count_query = `SELECT COUNT(*) FROM ${schema_name}."${view_name}"`;  // Use double quotes for view names
    try {
        var count_stmt = snowflake.createStatement({ sqlText: count_query });
        var count_result = count_stmt.execute();
        count_result.next();
        var row_count = count_result.getColumnValue(1);

        if (row_count == 0) {
            result.push(view_name + ' has no data.');
        } else {
            result.push(view_name + ' has data.');
        }
    } catch (e) {
        result.push(view_name + ' encountered an error: ' + e.message);
    }
}

return result.join('\n');
$$;

CALL check_views_data_warrrl();



CREATE OR REPLACE PROCEDURE check_views_data_coex_dwh()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = [];
var schema_name = 'COEX_DEV.DWH';  // Include the database name
var query = `
    SELECT table_name 
    FROM COEX_DEV.information_schema.views  -- Explicitly use COEX_DEV
    WHERE table_schema = 'DWH'
`;

var stmt = snowflake.createStatement({ sqlText: query });
var views = stmt.execute();

while (views.next()) {
    var view_name = views.getColumnValue(1);
    var count_query = `SELECT COUNT(*) FROM ${schema_name}."${view_name}"`;  // Use double quotes for view names
    try {
        var count_stmt = snowflake.createStatement({ sqlText: count_query });
        var count_result = count_stmt.execute();
        count_result.next();
        var row_count = count_result.getColumnValue(1);

        if (row_count == 0) {
            result.push(view_name + ' has no data.');
        } else {
            result.push(view_name + ' has data.');
        }
    } catch (e) {
        result.push(view_name + ' encountered an error: ' + e.message);
    }
}

return result.join('\n');
$$;

CALL check_views_data_coex_dwh();


CREATE OR REPLACE PROCEDURE check_views_data_warrrl_dwh()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = [];
var schema_name = 'WARRRL_DEV.DWH';  // Include the database name
var query = `
    SELECT table_name 
    FROM WARRRL_DEV.information_schema.views  -- Explicitly use WARRRL_DEV
    WHERE table_schema = 'DWH'
`;

var stmt = snowflake.createStatement({ sqlText: query });
var views = stmt.execute();

while (views.next()) {
    var view_name = views.getColumnValue(1);
    var count_query = `SELECT COUNT(*) FROM ${schema_name}."${view_name}"`;  // Use double quotes for view names
    try {
        var count_stmt = snowflake.createStatement({ sqlText: count_query });
        var count_result = count_stmt.execute();
        count_result.next();
        var row_count = count_result.getColumnValue(1);

        if (row_count == 0) {
            result.push(view_name + ' has no data.');
        } else {
            result.push(view_name + ' has data.');
        }
    } catch (e) {
        result.push(view_name + ' encountered an error: ' + e.message);
    }
}

return result.join('\n');
$$;

CALL check_views_data_warrrl_dwh();






