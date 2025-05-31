-- 1. Create a user in the DB from an AD group
CREATE USER [DATAHUB_STREAM_READERS_PROD] FROM EXTERNAL PROVIDER;

-- 2. Create a role in the DB
CREATE ROLE [DATAHUB-stream-reader-ROLE];

-- 3. Grant the role permissions to access a specific schema
GRANT SELECT ON SCHEMA::[STREAM_ANALYTICS] TO [DATAHUB-stream-reader-ROLE];

-- 4. Assign the user to the role
ALTER ROLE [DATAHUB-stream-reader-ROLE]
    ADD MEMBER [DATAHUB_STREAM_READERS_PROD];
GO

-- Instruct user to test access