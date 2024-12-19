USE CMI_MRSafetyDB;
GO

-- Create roles
CREATE ROLE PhysicsAdministrator;
CREATE ROLE Physicist;
CREATE ROLE Trainee;
CREATE ROLE Analyst;

-- Grant full control to the System Administrator role
GRANT CONTROL ON DATABASE::CMI_MRSafetyDB TO PhysicsAdministrator;
GO

-- Grant database access
GRANT CONNECT TO Physicist;
GRANT CONNECT TO Trainee;
GRANT CONNECT TO Analyst;
GO

-- Grant permissions to Physicist
GRANT SELECT, INSERT, UPDATE ON SCHEMA::dbo TO Physicist;
GO

-- Grant permissions to Trainee
GRANT SELECT ON SCHEMA::dbo TO Trainee;
GO

-- Grant permissions to Analust
GRANT SELECT ON SCHEMA::dbo TO Analyst;
DENY SELECT ON dbo.Patient TO Analyst; -- Deny permissions to Patient table
GRANT SELECT ON dbo.MasterSafetyWorklist_Pseudo TO Analyst;
DENY SELECT ON dbo.MasterSafetyWorklist_Full TO Analyst;
GO

-- Add data masking to sensitive columns
ALTER TABLE dbo.CRISComment
ALTER COLUMN Comment ADD MASKED WITH (FUNCTION = 'default()');

ALTER TABLE dbo.ActionLog
ALTER COLUMN ActionDescription ADD MASKED WITH (FUNCTION = 'default()');

ALTER TABLE dbo.Query
ALTER COLUMN QueryText ADD MASKED WITH (FUNCTION = 'default()');

-- Grant full access to unmasked data
GRANT UNMASK TO Physicist;
GRANT UNMASK TO PhysicsAdministrator;
GRANT UNMASK TO Trainee;
