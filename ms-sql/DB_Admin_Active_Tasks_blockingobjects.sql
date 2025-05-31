USE Master
GO
SELECT * 
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;
GO

OR

SP_Who2 'ACTIVE'