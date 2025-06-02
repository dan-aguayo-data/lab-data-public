

/*
Account_Usage views mirror the corresponding views and table functions in the Snowflake Information Schema, but with the following differences:
    Records for dropped objects included in each view
    Longer retention time for historical usage data
    Data latency
*/

// Capture the list of users in the account
Select 
    Name,
    Created_On,
    Deleted_On,
    Login_Name,
    Display_Name,
    First_Name,
    Last_Name,
    Email,
    Comment,
    Disabled,
    Must_Change_Password,   -- Specifies whether the user is forced to change their password on their next login.
    Snowflake_Lock,         -- Specifies whether a temporary lock has been placed on the user’s account.
    Default_Warehouse,      -- Specifies the virtual warehouse that is active by default for the user’s session upon login.
    Default_Namespace,      -- Specifies the namespace, database and schema, that is active by default for the user’s session upon login
    Default_Role,           -- Specifies the role that is active by default for the user’s session upon login.
    Ext_Authn_Duo,          -- If true the user is using MFA. Cannot be set directly.
    Ext_Authn_uid,          -- MFA user Id
    Last_Success_Login,     
    Expires_At,             -- Date and time the account expires
    Locked_Until_Time,      -- Specifies the number of minutes until the temporary lock on the user login is cleared
    Has_Password,           -- Specifies whether a password was created for the user
    Has_Rsa_Public_Key      -- Specifies whether RSA public key used for key pair authentication has been set up for the user
From Snowflake.Account_Usage.Users
WHERE DELETED_ON IS NOT NULL and name like 'ADRIAN%'

;
// Generate a list of Role
Select
    Created_On,
    Deleted_On,
    Name as Role_Name,
    Comment
From Snowflake.Account_Usage.Roles;

// List all privileges granted to any role
Select 
    Created_On,
    Deleted_On,
    Modified_On,
    Privilege,              -- The type of privilege assigned to the role
    Granted_On,             -- The scope (object) of the privilege
    Name as Object_Name,    -- Object name
    Table_Catalog,          -- Database name
    Table_Schema,           -- Schema name
    Granted_To,             -- This will always be 'ROLE' in this view
    Grantee_Name,           -- The name of the role to which the privilege was granted
    Grant_Option,           -- If this is true, the role can grant the privilege to other roles
    Granted_By              -- The name of the role that granted the privilege
From Snowflake.Account_Usage.Grants_To_Roles;

// List all roles granted to any user
Select
    Created_On,
    Deleted_On,
    Role,                       -- Name of the role
    Granted_To,                 -- This will always be 'USER' in this view
    Grantee_Name as User_Name,  -- User name receiving the grant
    Granted_By                  -- Role that granted the privilege
From Snowflake.Account_Usage.Grants_To_Users;

// List all grants on the account
Show Grants on Account;

// Same as above with a bit more flexibility
Show Grants on Account;
Select *
From table(result_scan(Last_Query_Id()));

// List all grants on the target object
Show Grants on Database Demo;

// List all privileges and roles granted to the target role
Show Grants To Role LKMRole;

// List all roles granted to the target user
Show Grants To User LaurenMinder;

// List all futuer grants in the target database
Show Future Grants in Database LaurenDatabase;

// Generate a list of all roles, privileges and who is assigned to each
With All_Roles_And_Privileges AS
(
  Select 
    Created_On,                 -- When was the role + privilege created?
    Modified_On,                -- When was the role + privilege last modifed?
    Privilege,                  -- What is the privilege?
    Granted_On,                 -- What object was the role + privilege granted on?
    Table_Catalog,              -- Associated database name
    Table_Schema,               -- Associated schema name
    Granted_To,                 -- What was the role + privilege granted to
    Grantee_Name as Role_Name,  -- What role was the role + privilege granted to
    Deleted_On                  -- When was this row deleted?
  From Snowflake.Account_Usage.Grants_To_Roles
),

Roles_Assigned_To_Users AS
(
    Select
        Created_On,                 -- When was the role assigned to the user?
        Deleted_On,                 -- When was this role deleted?
        Role as Role_Name,          -- Role name
        Granted_To,                 -- What was the role assigned to?
        Grantee_Name as User_Name,  -- Name of user the role was assigned to
        Granted_By                  -- Which role granted this?
    From Snowflake.Account_Usage.Grants_To_Users
)

Select t.*, t2.User_Name
From All_Roles_And_Privileges t
    Left Join Roles_Assigned_To_Users t2 On t.Role_Name = t2.Role_Name
Where t.Deleted_On IS NULL And t2.Deleted_On IS NULL;

// What is the current user, role, database and warehouse?
Select Current_User();
Select Current_Role();
Select Current_Warehouse();
Select Current_Database();

// Creating a User
// Set the context
USE ROLE SECURITYADMIN;

// Create user with password authentication
CREATE USER SarahMinder
  PASSWORD             = 'randomly-generated-password'
  LOGIN_NAME           = 'sarahminder@gmail.com'
  DISPLAY_NAME         = 'Sarah Minder'
  FIRST_NAME           = 'Sarah'
  LAST_NAME            = 'Minder'
  EMAIL                = 'sarahminder@gmail.com'
  MUST_CHANGE_PASSWORD = TRUE
  DEFAULT_ROLE         = Marketing;
  
// Grant usage on the default role
GRANT ROLE Marketing TO USER SarahMinder;
;

// Retrieve user login statistics 
Select
    event_timestamp,
    event_id,
    event_type,
    user_name,
    client_ip,
    reported_client_type,
    reported_client_version,
    first_authentication_factor,
    second_authentication_factor,
    is_success,
    error_message
From Snowflake.Account_Usage.Login_History
Where user_name = 'RANDYMINDER';    

// Retrieve the count of failed logins month-to-date
Select 
    user_name,
    Sum(iff(is_success = 'NO', 1, 0)) As failed_logins,
    Count(*) as logins,
    Sum(iff(is_success = 'NO', 1, 0)) / nullif(Count(*), 0) as login_failure_rate
From Snowflake.Account_Usage.Login_History
Where event_timestamp > date_trunc(month, current_date)
    Group By user_name
    Order By login_failure_rate desc;

// Calculate the average number of seconds between failed login attempts by each user, 
// month-to-date
With Logins AS
(
  Select 
    user_name,
    timediff(seconds, event_timestamp, Lead(event_timestamp) Over(partition by user_name order by event_timestamp)) as seconds_between_login_attempts
  From Snowflake.Account_Usage.Login_History
  Where event_timestamp > date_trunc(month, current_date) And is_success = 'NO'
)

Select 
    user_name,
    Count(*) as failed_logins,
    Avg(seconds_between_login_attempts) as average_seconds_between_login_attempts
From Logins
    Group By user_name
    Order By average_seconds_between_login_attempts;

// Calculate the average query execution time for each user in your account
// month-to-date
Select 
    user_name,
    Avg(execution_time) as average_execution_time -- In milliseconds
From Snowflake.Account_Usage.Query_History
Where start_time >= date_trunc(month, current_date)
Group By user_name
Order By 2 Desc;

// Calculate a query count for each user session (login)
Select 
    l.user_name,
    l.event_id,
    Count(q.query_id)
From Snowflake.Account_Usage.Login_History l
    Join Snowflake.Account_Usage.Sessions s On l.event_id = s.login_event_id
    Join Snowflake.Account_Usage.Query_History q On q.session_id = s.session_id
    Group By 1, 2
    Order By l.user_name;

// Retrieve the query history for a user
 Select
    query_id,
    query_text,
    database_name,
    schema_name,
    query_type,
    session_id,
    user_name,
    role_name,
    warehouse_name,
    warehouse_size,
    query_tag,                          -- Every query in Snowflake can be tagged
    execution_status,                   -- Execution status for the query: success, fail, incident
    error_message,
    start_time,
    end_time,
    total_elapsed_time,                 -- In milliseconds
    bytes_scanned,                      -- Number of bytes scanned by this statement
    percentage_scanned_from_cache,      -- The percentage of data scanned from the local disk cache. The value ranges from 0.0 to 1.0 (100%)
    bytes_written,                      -- Number of bytes written (e.g. when loading into a table)
    rows_produced,                      -- Number of rows produced by this statement
    rows_inserted,
    rows_updated,
    rows_deleted,
    rows_unloaded,
    bytes_spilled_to_local_storage,     -- Volume of data spilled to local disk (warehouse node)
    bytes_spilled_to_remote_storage,    -- Volume of data spilled to remote disk (cloud provider storage)
    bytes_sent_over_the_network,        -- Volume of data sent over the network
    compilation_time,                   -- In milliseconds
    execution_time,                     -- In milliseconds
    queued_provisioning_time,           -- Time (milliseconds) spent in the warehouse queue, waiting for the warehouse servers to provision, due to warehouse creation, resume, or resize
    queued_repair_time,                 -- Time (milliseconds) spent in the warehouse queue, waiting for servers in the warehouse to be repaired
    queued_overload_time,               -- Time (milliseconds) spent in the warehouse queue, due to the warehouse being overloaded by the current query workload
    transaction_blocked_time,           -- Time (milliseconds) spent blocked by a concurrent DML operation
    credits_used_cloud_services,        -- Credits consumed
    release_version,                    -- Release version of Snowflake
    query_load_percent                  -- Percentage of load this query put on the associated warehouse
 From Snowflake.Account_Usage.Query_History
 Where user_name = 'RANDYMINDER';
