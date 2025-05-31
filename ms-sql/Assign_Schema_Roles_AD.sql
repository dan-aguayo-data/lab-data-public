
-- 1.  Create a user in the DB and give them access to a certain schema... User is Always a AD group that has been set up.
CREATE USER [PRW_NEBULA_ZZINT_ANAPLAN_READER_PROD] FROM EXTERNAL PROVIDER  -- PRW_NEBULA_ZZINT_ANAPLAN_READER_PROD    --PRW_NEBULA_REVUP_READER_PROD

-- 2.  Create Role in the DB
CREATE ROLE [PRW-nbl-ZZINTANAPLAN-reader-ROLE]

-- 3.  Give the role permissiosn to access a certain schema

GRANT SELECT ON SCHEMA::[ZZINT_ANAPLAN] TO [PRW-nbl-ZZINTANAPLAN-reader-ROLE]

-- 4.  Assign the User to the relevant roles

ALTER ROLE [PRW-nbl-ZZINTANAPLAN-reader-ROLE]
       ADD MEMBER [PRW_NEBULA_ZZINT_ANAPLAN_READER_PROD]
GO

-- Done get user to test the access
