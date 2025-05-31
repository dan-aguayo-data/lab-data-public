declare
  res RESULTSET;                             -- Declares a variable to hold the result set.
  rpt VARIANT;                               -- Declares a variable to hold the report data.
  name VARCHAR;                              -- Declares a variable to hold the stage name.
  err varchar := '';                         -- Declares a variable to capture any error messages, initialized to an empty string.
  total_size NUMBER;                         -- Declares a variable to hold the total size of files in a stage.
  res_query VARCHAR DEFAULT 'select KEY STAGE_NAME, VALUE TOTAL_BYTES from table(flatten(parse_json(?))) order by 2 DESC';  
                                             -- Declares a variable with the query to transform the JSON report into a result set.
  c1 cursor for select concat_ws('.', stage_catalog, stage_schema, stage_name) name 
                 from snowflake.account_usage.stages 
                 where stage_type = 'Internal Named' and deleted is NULL; 
                                             -- Declares a cursor to iterate over internal named stages that are not deleted.
begin
  rpt := object_construct();                 -- Initializes an empty JSON object to hold the report data.
  for record in c1 do                        -- Starts a loop over each stage in the cursor c1.
      begin
          name := record.name;               -- Assigns the stage name to the variable name.
          res := (execute immediate 'ls @' || name);  -- Executes the 'ls' command to list files in the current stage and stores the result in res.
          let c2 cursor for res;             -- Declares a cursor c2 to iterate over the result set res.
          total_size := 0;                   -- Initializes the total_size to 0 for the current stage.
          for inner_record in c2 do          -- Starts a loop over each file in the result set c2.
              begin
                  total_size := total_size + inner_record."size";  -- Adds the size of each file to the total_size.
              EXCEPTION 
                  WHEN OTHER THEN
                      err := concat_ws('\n', err, SQLERRM);  -- Captures any error during file size summation and appends it to err.
              end;
           end for;
           rpt := object_insert(rpt, name, total_size);  -- Inserts the total size of the current stage into the rpt JSON object.
      EXCEPTION 
          WHEN OTHER THEN
              err := concat_ws('\n', err, SQLERRM);  -- Captures any error during the stage processing and appends it to err.
      end;
  end for;
  res := (execute immediate :res_query using (rpt));  -- Executes the res_query to transform the rpt JSON object into a result set.
  return table(res);                                  -- Returns the result set as a table.
  --return err;                                        -- This line is commented out, but it could return the error messages if needed.
end;
