
--1. GENERATE NEW CERTIFICATE
--Generate private key
openssl genpkey -algorithm RSA -out private_key.pem

-- Generate a certificate signing request (CSR)
openssl req -new -key private_key.pem -out certificate_request.csr

-- Generate a self-signed certificate
openssl x509 -req -in certificate_request.csr -signkey private_key.pem -out certificate.pem -days 365



--2.UPLOAD NEW CERTIFICATE TO AZURE AD
-- Go to Azure Active Directory: Log in to the Azure portal and navigate to Azure Active Directory.
-- App Registrations: Go to "App registrations" and select your Snowflake application.
-- Certificates & secrets: Select "Certificates & secrets" from the left-hand menu.
-- Upload Certificate: Click "Upload certificate" and upload your new certificate.pem.


--3. Update the Snowflake SCIM Security Integration.

--# Extract public key from certificate
openssl x509 -pubkey -noout -in certificate.pem > public_key.pem

--# Extract thumbprint
openssl x509 -noout -fingerprint -sha1 -in certificate.pem | sed 's/://g' | sed 's/SHA1 Fingerprint=//g'



-- Replace the integration name, public key, and thumbprint with your values
ALTER SECURITY INTEGRATION AZUREADINTEGRATION
SET
   SCIM_CLIENT_CERT = '<contents_of_public_key.pem>',
   SCIM_CLIENT_CERT_FINGERPRINT = '<thumbprint>';


-- ALTER SECURITY INTEGRATION AZUREADINTEGRATION
-- SET
--    SCIM_CLIENT_CERT = 'MIIBIXXXXXXXXXQAB',
--    SCIM_CLIENT_CERT_FINGERPRINT = '3DF4A567BB1AXXXXXXXXXA4B0C123';


-- use role accountadmin;
-- select system$generate_scim_access_token('AAD_PROVISIONING'); 
-- --use the name of your SCIM integration



-- USE ROLE ACCOUNTADMIN;

-- create role if not exists aad_provisioner ;
-- grant create user on account to role aad_provisioner ;
-- grant create role on account to role aad_provisioner ;
-- grant role aad_provisioner to role accountadmin ;
-- create or replace security integration aad_provisioning
-- type = scim
-- scim_client = 'azure'
-- run_as_role = 'ADD_PROVISIONER' ;

-- select system$generate_scim_access_token('AAD_PROVISIONING') ; --THIS WILL GENERATE THE TOKEN,




   ;

   SHOW SECURITY INTEGRATIONS ; 


   SHOW USERS ;

   describe user "ces_dbt_sa" ;



   use role accountadmin;
show parameters like '%SAML_IDENTITY_PROVIDER%' in account;


use role accountadmin;
show integrations;


desc security integration AAD_PROVISIONING;


-- ATP table Payment terms log table 


--use role accountadmin;

/*STEP 1*/
--SYSTEM$MIGRATE_SAML_IDP_REGISTRATION( 'AAD_PROVISIONING', 'https://XXXXXXX.australia-east.azure.snowflakecomputing.com' )

--https://ur45154.australia-east.azure.snowflakecomputing.com/fed/login

/*STEP 2*/
--desc security integration 'AAD_PROVISIONING';

/*STEP 3 */

--alter security integration 'AAD_PROVISIONING' set SAML2_X509_CERT = 'string_literal';




-- Find the SAML2_SNOWFLAKE_X509_CERT value in row 7, which is the public certificate in PEM format.

-- Save the value, ensuring you include the BEGIN CERTIFICATE and END CERTIFICATE delimiters. For example, the codeblock below contains a truncated certificate in PEM format:

-- -----BEGIN CERTIFICATE-----
-- MIICr...
-- -----END CERTIFICATE-----


-- ALTER ACCOUNT SET
-- SAML_IDENTITY_PROVIDER = '{
-- "certificate": "MIIC8DXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
-- "type": "custom",
-- "label":"AzureAD"
-- }';

-- alter account set sso_login_page = true ;


DESC SECURITY INTEGRATION AZUREADINTEGRATION;


DESCRIBE SECURITY INTEGRATION AZUREADINTEGRATION ;

show security integrations;
-- -- Replace the integration name, public key, and thumbprint with your values
-- ALTER SECURITY INTEGRATION AZUREADINTEGRATION
-- SET
--    ENABLED = TRUE,
--    AZURE_AD_APPLICATION_NAME = '<your-azure-app-name>',
--    AZURE_AD_CERT = '<contents_of_public_key.pem>',
--    AZURE_AD_CERT_THUMBPRINT = '<thumbprint>';
