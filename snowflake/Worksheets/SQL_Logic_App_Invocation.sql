use role accountadmin;


show api integrations;  -- EXTERNAL_API_INTEGRATION

    
describe api integration EXTERNAL_API_INTEGRATION;



-- https://docs.snowflake.com/en/sql-reference/external-functions-creating-azure-common-ext-function

/*CREATE FUNCTION*/
--- create working azure function

/*MODIFY SETTING IN FUNCTION*/
-- create remote service by configuring Settings and click on Authentication of function app, this will create an azure_ad_application_id

/*ADD FUNCTION TO AN API MANAGEMENT SERVICE*/
-- create proxy to the serie api under api management, in the future you will need the Api management service name


/*RUN THIS IN SNOWFLAKE*/

create or replace api integration AZURE_EXTERNAL_FUNCTION
    api_provider = azure_api_management
    azure_tenant_id = '<tenant_id>'   --abf0de66-3ab3-4ea3-b35e-502502be1261
    azure_ad_application_id = '<azure_application_id>'  -- requires higher level of authorization 
    api_allowed_prefixes = ('<url>')   -- this comes fr om the api managemetnt service url
    enabled = true;


/*RECORD THE AZURE CONSENT URL */

    describe api integration AZURE_EXTERNAL_FUNCTION;


-- Record the app name (from the AZURE_MULTI_TENANT_APP_NAME column) in the corresponding field in your tracking worksheet.
-- Record the consent URL (from the AZURE_CONSENT_URL column) in the corresponding field in your tracking worksheet.
-- The URL looks similar to the following:
-- https://login.microsoftonline.com/<tenant_id>/oauth2/authorize?client_id=<snowflake_application_id>&response_type=code


/*GRANT PERMISSION TO SF */

-- you need consent url from the describe AZURE_CONSENT_URL to grant permission to snowflake

-- Paste the URL into your browser. When your browser resolves this URL, Azure automatically creates a service principal that represents Snowflake in the tenant.


/*CREATE EXTERNAL FUNCTION IN SNOWFLAKE */

create or replace external function logic_app_invocation(<parameters>)
    returns variant
    api_integration = <api_integration_name>
    as '<invocation_url>';

--Replace <external_function_name> with a unique function name (e.g. echo). This name must follow the rules for Object identifiers.
--In addition, record the function name in the External Function Name field in your tracking worksheet.

--Replace <parameters> with the names and SQL data types of the parameters for the function, if any.
--The parameters must correspond to the parameters expected by the remote service. The parameter names do not need to match, but the data types need to be compatible.
--If your Azure Function uses the sample JavaScript code provided in Step 1, then the parameters are an INTEGER and a VARCHAR. For example: a integer, b varchar

--Replace <api_integration_name> with the value from the API Integration Name field in your tracking worksheet.

    
SHOW EXTERNAL FUNCTIONS ;

/*Finally Update Security Policy as step 6 indicates*/


/*USE YOUR FUNCTION */
--GRANTE PERMISSIONS
GRANT USAGE ON FUNCTION <external_function_name>(<parameter_data_type>) TO <role_name>;


--CALL Azure function parameters

select logic_app_invocation('https%3A%2F%2Fprod-19.australiaeast.logic.azure.com%3A443%2Fworkflows%2F54e6d3e3edb24e7dac1ca57eca45700d%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Fpaths%2Finvoke%3Fapi-version%3D2016-10-01%26sp%3D%252Ftriggers%252FWhen_a_HTTP_request_is_received%252Frun%26sv%3D1.0%26sig%3DyVyxWPIoWDWqCJ2C5YUAQIW-ZUFsindF4nM9N-wqj1Q')