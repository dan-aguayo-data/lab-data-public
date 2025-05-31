USE ROLE ACCOUNTADMIN;

DROP SECURITY INTEGRATION AZUREADINTEGRATION;

CREATE SECURITY INTEGRATION AZUREADINTEGRATION
TYPE = SAML2
ENABLED = TRUE
SAML2_ISSUER = 'https://sts.windows.net/abf0de66-3ab3-4ea3-b35e-502502be1261/' 
SAML2_SSO_URL = 'https://login.microsoftonline.com/abf0de66-3ab3-4ea3-b35e-502502be1261/saml2'
SAML2_PROVIDER = 'CUSTOM'
SAML2_X509_CERT = 'MIIC8DCCAdigAwIBAgIQWtBn3jBfM71IIW5JZ3I81DANBgkqhkiG9w0BAQsFADA0MTIwMAYDVQQDEylNaWNyb3NvZnQgQXp1cmUgRmVkZXJhdGVkIFNTTyBDZXJ0aWZpY2F0ZTAeFw0yNDA3MjMwNDM5MjRaFw0yNzA3MjMwNDM5MjRaMDQxMjAwBgNVBAMTKU1pY3Jvc29mdCBBenVyZSBGZWRlcmF0ZWQgU1NPIENlcnRpZmljYXRlMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzp87ho9pNyWNuElWaYI8skZSD8hvnTg5Dy0fgftAwEslICVUB5js+Mnyv3pY6aQ9uXNvsfJza5ZWIcznJjuT5/P77S24/CHpRrsl4UJLOBjRP/R8tWuJYMUhLrMUR6Xn392gtiLzULXFQ69nKQAXl7iBu+KOJx6KrQR+q0jcjnFE9VkIEPXQ7ZjNwKIDwAYWG3rqttWhw8UZfN2Zm0qy0N+BCV5hyQ30j6GRyW1RSItM6MgyIzZYDwg1GNH6kihOXZgmqv8XLaRa4GxUQakn5JL9FCGHmfM+fb3PRwIx6ldt0lk5pcpJYknNOr7FhrTUZ9dfi8GopMOnNf1HiDr1jQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQB8pcPYucwAHRk5+Bs4nDrWQnFG0cs4FysABPACgUyIRtK3taBakqMAbsyI/SWMgxhDcyPm6k3nv6HOJRKZC/2A0Hk0jclR8ey/wz/snKyKlWWkb68I49Ztr4U6IkkfH5iBGqgou/2SANE25mzBpX3tc8ny5pcXc6hB487urrgee8mA0BCORJthuhLKplhifwqpfXTIrd52SoRrorbzs1V2A8YH4L8kNUmzN55DbCyvsnq0JnnrIHd5Kvb2ZL+cxR8CAp6wHlUYsMVDgr5GVtaXUdAvuFxDrkJtTyDodu2UnV2J9TZul88T0uwh/yGyfZxF1KtCkLT1suDXc+mqRztA' 
SAML2_SP_INITIATED_LOGIN_PAGE_LABEL = 'AzureADSSO'
SAML2_ENABLE_SP_INITIATED = TRUE;

DESC SECURITY INTEGRATION AAD_PROVISIONING;
DESC SECURITY INTEGRATION AZUREADINTEGRATION;

USE ROLE ACCOUNTADMIN;

ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ACS_URL = 'https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com/fed/login';
ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ISSUER_URL = 'https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com';

ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ACS_URL = 'https://cd63105.ap-southeast-2.snowflakecomputing.com/fed/login';
ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ISSUER_URL = 'https://cd63105.ap-southeast-2.snowflakecomputing.com';

BP43112-hu99273.snowflakecomputing.com


AAD_PROVISIONING

cd63105.ap-southeast-2.privatelink.snowflakecomputing.com
https://cd63105.ap-southeast-2.snowflakecomputing.com/fed/login

use role accountadmin;
show parameters like '%SAML_IDENTITY_PROVIDER%' in account;


ALTER SECURITY INTEGRATION AAD_PROVISIONING SET NETWORK_POLICY = 'ALLOW_AZUREAD_SCIM';
ALTER SECURITY INTEGRATION AAD_PROVISIONING SET NETWORK_POLICY = 'AWS_PRIVATE_LINK_ONLY';

SHOW integrations

https://community.snowflake.com/s/article/HOW-TO-Setup-SSO-with-Azure-AD-and-the-Snowflake-New-URL-Format-or-Privatelink

alter security integration AAD_PROVISIONING set saml2_snowflake_acs_url = 'https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com/fed/login';

select system$generate_scim_access_token('AAD_PROVISIONING')

SELECT CURRENT_ACCOUNT_NAME();

DROP NETWORK RULE AZURE_AD_WHITE_LIST
CREATE NETWORK RULE AZURE_AD_WHITE_LIST
  TYPE = IPV4
  VALUE_LIST =( "4.149.98.192/27",
                    "4.149.105.224/27",
                    "4.150.253.96/28",
                    "4.156.6.96/28",
                    "4.190.147.80/28",
                    "4.198.160.112/28",
                    "4.207.244.176/28",
                    "13.64.151.161/32",
                    "13.66.141.64/27",
                    "13.66.150.240/28",
                    "13.67.9.224/27",
                    "13.67.21.96/27",
                    "13.69.66.160/27",
                    "13.69.119.208/28",
                    "13.69.229.96/27",
                    "13.70.73.32/27",
                    "13.71.172.160/27",
                    "13.71.195.224/27",
                    "13.71.201.64/26",
                    "13.73.240.32/27",
                    "13.74.104.0/26",
                    "13.74.203.80/28",
                    "13.74.249.156/32",
                    "13.75.38.32/27",
                    "13.75.105.168/32",
                    "13.77.52.160/27",
                    "13.78.108.192/27",
                    "13.78.172.246/32",
                    "13.79.37.247/32",
                    "13.86.219.0/27",
                    "13.87.16.0/26",
                    "13.87.57.160/27",
                    "13.87.123.160/27",
                    "13.89.174.0/27",
                    "20.18.183.32/28",
                    "20.20.32.0/19",
                    "20.36.107.192/27",
                    "20.36.115.64/27",
                    "20.36.151.160/28",
                    "20.37.75.96/27",
                    "20.40.228.64/28",
                    "20.42.79.112/28",
                    "20.43.120.32/27",
                    "20.43.127.160/28",
                    "20.44.3.160/27",
                    "20.44.16.32/27",
                    "20.46.10.64/27",
                    "20.50.76.176/28",
                    "20.50.206.128/28",
                    "20.51.9.80/28",
                    "20.51.14.72/31",
                    "20.51.16.128/27",
                    "20.61.98.160/27",
                    "20.61.99.128/28",
                    "20.62.58.80/28",
                    "20.62.129.0/27",
                    "20.62.129.240/28",
                    "20.62.134.74/31",
                    "20.65.4.192/28",
                    "20.65.132.96/28",
                    "20.66.2.32/27",
                    "20.66.3.16/28",
                    "20.72.21.64/26",
                    "20.83.195.128/28",
                    "20.88.66.0/27",
                    "20.89.1.112/30",
                    "20.111.78.128/28",
                    "20.150.227.112/28",
                    "20.187.197.32/27",
                    "20.187.197.240/28",
                    "20.190.128.0/18",
                    "20.192.102.240/28",
                    "20.194.70.224/27",
                    "20.194.73.0/28",
                    "20.194.129.224/28",
                    "20.195.56.102/32",
                    "20.195.57.118/32",
                    "20.195.64.192/27",
                    "20.195.64.240/28",
                    "20.195.138.96/28",
                    "20.195.154.64/28",
                    "20.205.195.32/27",
                    "20.207.219.224/27",
                    "20.222.128.176/28",
                    "20.231.128.0/19",
                    "23.98.114.128/27",
                    "23.101.0.70/32",
                    "23.101.6.190/32",
                    "40.64.116.112/28",
                    "40.68.160.142/32",
                    "40.69.107.160/27",
                    "40.71.13.0/27",
                    "40.74.99.80/28",
                    "40.74.101.64/27",
                    "40.74.146.192/27",
                    "40.78.195.160/27",
                    "40.78.203.64/27",
                    "40.79.131.128/27",
                    "40.79.179.128/27",
                    "40.80.55.32/28",
                    "40.83.144.56/32",
                    "40.115.144.8/29",
                    "40.124.67.224/27",
                    "40.126.0.0/18",
                    "51.11.194.64/28",
                    "51.12.33.144/28",
                    "51.12.231.32/28",
                    "51.12.238.224/28",
                    "51.140.148.192/27",
                    "51.140.208.0/26",
                    "51.140.211.192/27",
                    "52.138.65.157/32",
                    "52.138.68.41/32",
                    "52.138.229.112/28",
                    "52.146.132.96/27",
                    "52.146.133.80/28",
                    "52.146.137.66/31",
                    "52.150.157.0/27",
                    "52.157.20.148/32",
                    "52.157.20.186/32",
                    "52.157.20.205/32",
                    "52.159.175.31/32",
                    "52.159.175.117/32",
                    "52.159.175.121/32",
                    "52.161.13.71/32",
                    "52.161.13.95/32",
                    "52.161.110.169/32",
                    "52.162.110.96/27",
                    "52.167.149.32/28",
                    "52.169.125.119/32",
                    "52.169.218.0/32",
                    "52.174.189.149/32",
                    "52.175.18.134/32",
                    "52.178.27.112/32",
                    "52.179.122.218/32",
                    "52.179.126.223/32",
                    "52.180.177.87/32",
                    "52.180.179.108/32",
                    "52.180.181.61/32",
                    "52.180.183.8/32",
                    "52.182.146.96/28",
                    "52.187.19.1/32",
                    "52.187.113.48/32",
                    "52.187.117.83/32",
                    "52.187.120.237/32",
                    "52.225.184.198/32",
                    "52.225.188.89/32",
                    "52.226.169.40/32",
                    "52.226.169.45/32",
                    "52.226.169.53/32",
                    "52.231.19.128/27",
                    "52.231.56.32/27",
                    "52.231.147.192/27",
                    "52.249.207.8/32",
                    "52.249.207.23/32",
                    "52.249.207.27/32",
                    "65.52.251.96/27",
                    "68.218.170.224/27",
                    "68.220.90.176/28",
                    "74.227.136.96/27",
                    "98.66.133.128/28",
                    "104.40.84.19/32",
                    "104.40.87.209/32",
                    "104.40.156.18/32",
                    "104.40.168.0/26",
                    "104.40.170.224/28",
                    "104.41.159.212/32",
                    "104.45.138.161/32",
                    "104.46.178.128/27",
                    "104.208.18.96/28",
                    "104.211.147.160/27",
                    "168.61.245.72/29",
                    "172.172.255.96/28",
                    "172.173.10.80/28",
                    "172.173.16.112/28",
                    "172.173.24.112/28",
                    "172.183.237.0/26",
                    "172.208.164.32/29",
                    "172.210.219.32/28",
                    "172.211.123.224/28",
                    "191.233.204.160/27",
                     "13.64.151.161/32",
                    "13.66.141.64/27",
                    "13.67.9.224/27",
                    "13.69.66.160/27",
                    "13.69.229.96/27",
                    "13.70.73.32/27",
                    "13.71.172.160/27",
                    "13.71.195.224/27",
                    "13.73.240.32/27",
                    "13.74.249.156/32",
                    "13.75.38.32/27",
                    "13.75.105.168/32",
                    "13.77.52.160/27",
                    "13.78.108.192/27",
                    "13.78.172.246/32",
                    "13.79.37.247/32",
                    "13.86.219.0/27",
                    "13.87.57.160/27",
                    "13.87.123.160/27",
                    "13.89.174.0/27",
                    "20.36.107.192/27",
                    "20.36.115.64/27",
                    "20.37.75.96/27",
                    "20.43.120.32/27",
                    "20.44.3.160/27",
                    "20.44.16.32/27",
                    "20.46.10.64/27",
                    "20.51.16.128/27",
                    "20.61.98.160/27",
                    "20.62.129.0/27",
                    "20.66.2.32/27",
                    "20.72.21.64/27",
                    "20.187.197.32/27",
                    "20.190.165.48/29",
                    "20.190.166.48/29",
                    "20.190.166.112/29",
                    "20.190.166.176/29",
                    "20.195.56.102/32",
                    "20.195.57.118/32",
                    "20.195.64.192/27",
                    "23.101.0.70/32",
                    "23.101.6.190/32",
                    "40.68.160.142/32",
                    "40.69.107.160/27",
                    "40.71.13.0/27",
                    "40.74.101.64/27",
                    "40.74.146.192/27",
                    "40.78.195.160/27",
                    "40.78.203.64/27",
                    "40.79.131.128/27",
                    "40.79.179.128/27",
                    "40.83.144.56/32",
                    "40.126.37.48/29",
                    "40.126.38.48/29",
                    "40.126.38.112/29",
                    "40.126.38.176/29",
                    "51.140.148.192/27",
                    "51.140.211.192/27",
                    "52.138.65.157/32",
                    "52.138.68.41/32",
                    "52.146.132.96/27",
                    "52.150.157.0/27",
                    "52.161.13.71/32",
                    "52.161.13.95/32",
                    "52.161.110.169/32",
                    "52.162.110.96/27",
                    "52.169.125.119/32",
                    "52.169.218.0/32",
                    "52.174.189.149/32",
                    "52.175.18.134/32",
                    "52.178.27.112/32",
                    "52.179.122.218/32",
                    "52.179.126.223/32",
                    "52.180.177.87/32",
                    "52.180.179.108/32",
                    "52.180.181.61/32",
                    "52.180.183.8/32",
                    "52.187.19.1/32",
                    "52.187.113.48/32",
                    "52.187.117.83/32",
                    "52.187.120.237/32",
                    "52.225.184.198/32",
                    "52.225.188.89/32",
                    "52.231.19.128/27",
                    "52.231.147.192/27",
                    "65.52.251.96/27",
                    "104.40.84.19/32",
                    "104.40.87.209/32",
                    "104.40.156.18/32",
                    "104.41.159.212/32",
                    "104.45.138.161/32",
                    "104.46.178.128/27",
                    "104.211.147.160/27",
                    "191.233.204.160/27",
                    "20.37.194.0/24")
  
  COMMENT ='AZURE_AD_WHITE_LIST';


  https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com/

DESC SECURITY INTEGRATION AZUREADINTEGRATION;

USE ROLE ACCOUNTADMIN;


ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ACS_URL = 'https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com/fed/login';

ALTER SECURITY INTEGRATION AZUREADINTEGRATION SET SAML2_SNOWFLAKE_ISSUER_URL = 'https://cd63105.ap-southeast-2.privatelink.snowflakecomputing.com';



use role accountadmin;
create or replace role aad_provisioner;
grant create user on account to role aad_provisioner;
grant create role on account to role aad_provisioner;
grant role aad_provisioner to role accountadmin;
create or replace security integration aad_provisioning
    type = scim
    scim_client = 'azure'
    run_as_role = 'AAD_PROVISIONER';
select system$generate_scim_access_token('AAD_PROVISIONING');


select system$get_privatelink_config();


{"regionless-snowsight-privatelink-url":"app-bp43112-hu99273.privatelink.snowflakecomputing.com","privatelink-account-name":"cd63105.ap-southeast-2.privatelink","privatelink-vpce-id":"com.amazonaws.vpce.ap-southeast-2.vpce-svc-0380d3f0ef9398145","snowsight-privatelink-url":"app.ap-southeast-2.privatelink.snowflakecomputing.com","regionless-privatelink-ocsp-url":"ocsp.bp43112-hu99273.privatelink.snowflakecomputing.com","privatelink-account-url":"cd63105.ap-southeast-2.privatelink.snowflakecomputing.com","regionless-privatelink-account-url":"bp43112-hu99273.privatelink.snowflakecomputing.com","privatelink_ocsp-url":"ocsp.cd63105.ap-southeast-2.privatelink.snowflakecomputing.com"}


"privatelink-vpce-id":"com.amazonaws.vpce.ap-southeast-2.vpce-svc-0380d3f0ef9398145"


select SYSTEM$GET_PRIVATELINK_AUTHORIZED_ENDPOINTS();


[ {
  "endpointId" : "123963708064",
  "endpointIdType" : "Aws Id"
}, {
  "endpointId" : "330977107317",
  "endpointIdType" : "Aws Id"
}, {
  "endpointId" : "398662845025",
  "endpointIdType" : "Aws Id"
} ]



  