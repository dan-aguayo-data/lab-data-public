CREATE OR REPLACE PROCEDURE check_views_data(database_name STRING, schema_name STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = [];

var full_schema_name = database_name + '.' + schema_name;

var query = `
    SELECT table_name 
    FROM ${database_name}.information_schema.views 
    WHERE table_schema = '` + schema_name + `'`;

var stmt = snowflake.createStatement({ sqlText: query });
var views = stmt.execute();

while (views.next()) {
    var view_name = views.getColumnValue(1);
    var count_query = `SELECT COUNT(*) FROM ${full_schema_name}."${view_name}"`;

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
