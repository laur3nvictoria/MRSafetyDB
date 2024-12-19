/*
===========================================
-- Script Name:    Create_Tables_MRSafetyDB.sql
-- Description:    Creates tables for the MR Safety Queries database
-- Author:         Lauren Willis
-- Date Created:   2024-11-12
-- Last Modified:  2024-11-13
-- Version:        1.0
-- Database:       CMI_MRSafetyDB
===========================================
-- Purpose:
-- - This script creates tables needed for the MR safety query system.
-- - Includes tables: jobs, queries, staff, urgency, implant_type, etc.
-- - Adds foreign key constraints and indexes.
--
-- Changes:
-- - 2024-11-12 - Lauren Willis - Initial creation
-- - 2024-11-13 - Lauren Willis - Added 
-- - 2024-11-15 - Lauren Willis - Dropped column TypeCode from PatientType & renamed 
===========================================
*/

USE CMI_MRSafetyDB
GO

/****CREATE PATIENT TABLE****/

CREATE TABLE Patient (
	PatientId INT PRIMARY KEY IDENTITY(1,1),	--Auto-incrementing primary key
	HospitalNumber NVARCHAR(7),					--Max length of 7 characters, either A123456 or 123456
	NHSNumber NCHAR(10),						--Fixed length of 10 digits, will add constraint for only digits
	Initials NVARCHAR(5),						--Assuming no more than 5 characters for patient initials e.g. ABC
	PatientCode AS (							--Computed column that uses either hospital number or NHS number as primary identifier for patient
		CASE
			WHEN NHSNumber IS NULL THEN 'RK9' + HospitalNumber	--If NHS number is NULL, concatenate hospital number with ODS code 'RK9' for UHPNT
			WHEN HospitalNumber IS NULL THEN NHSNumber			--If hospital number is NULL, use NHS number
			ELSE NHSNumber										--If both NHS number and hospital number, use NHS number as primary identifier (as per Trust policy)
		END
	) PERSISTED									-- Store computed value in table, improves performance
);

/****ADD CONSTRAINTS TO PATIENT TABLE****/

ALTER TABLE Patient
ADD CONSTRAINT NHSNumConstraint					--NHS Number
CHECK(NHSNumber NOT LIKE '%[^0-9]%');			--Check if NHS number 10 digits long, 1234567890

ALTER TABLE Patient
ADD CONSTRAINT HospitalNumConstraint			--Hospital Number
CHECK (
	HospitalNumber LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]'		--Check if hospital number in format 123456
	OR
	HospitalNumber LIKE '[A-Z][0-9][0-9][0-9][0-9][0-9][0-9]'	--Check if hospital number in format A123456
);

ALTER TABLE Patient
ADD CONSTRAINT UC_PatientCode UNIQUE (PatientCode);

CREATE UNIQUE INDEX IX_HospitalNumber_Unique
ON dbo.Patient(HospitalNumber)
WHERE HospitalNumber IS NOT NULL;

CREATE UNIQUE INDEX IX_NHSNumber_Unique
ON dbo.Patient(NHSNumber)
WHERE NHSNumber IS NOT NULL;

/****CREATE PATIENT TYPE TABLE****/

CREATE TABLE PatientType (
	PatientTypeId INT PRIMARY KEY IDENTITY(1,1),	--Auto-incrementing primary key
	PatientTypeName NVARCHAR(50) NOT NULL			--Patient type description inpatient, outpatient or not specified
);

/****ALTER PATIENT TYPE TABLE****/
ALTER TABLE PatientType
ADD PatientTypeCode NVARCHAR(10) 					--Patient type IN, OUT or N/S

/****CREATE STAFF TABLE****/
-- Table Description: Staff table holds the information of staff in the MRI Physics team to identify which member(s) of staff completed each job or X-ray check

CREATE TABLE Staff (
	StaffId INT PRIMARY KEY IDENTITY(1,1),		--Auto-incrementing primary key
	FirstName NVARCHAR(255),					
	LastName NVARCHAR(255),
	Initials NVARCHAR(10) NOT NULL				--Staff initials are primary identifier of staff in current spreadsheet
);

ALTER TABLE Staff ADD CONSTRAINT UQ_Staff_Initials UNIQUE (Initials);
ALTER TABLE Staff ADD CONSTRAINT UQ_Staff_FirstName_LastName UNIQUE (FirstName, LastName);


/****CREATE SITE TABLE****/

CREATE TABLE Site (
	SiteId INT PRIMARY KEY IDENTITY(1,1),		--Auto-incrementing primary key
	SiteShortName NVARCHAR(50),
	SiteLongName NVARCHAR(255)
);
ALTER TABLE Site ADD CONSTRAINT UQ_Site_SiteShortName UNIQUE (SiteShortName);

/****CREATE URGENCY TABLE****/

CREATE TABLE Urgency (
	UrgencyId INT PRIMARY KEY IDENTITY(1,1),	--Auto-incrementing primary key
	UrgencyCode INT NOT NULL,
	UrgencyType NVARCHAR(50),
	UrgencyDescription NVARCHAR(255)
);

ALTER TABLE Urgency ADD CONSTRAINT UQ_Urgency_UrgencyCode UNIQUE (UrgencyCode);

/****CREATE IMPLANT TYPE TABLE****/

CREATE TABLE ImplantCategory (
	ImplantId INT PRIMARY KEY IDENTITY(1,1),	--Auto-incrementing primary key
	ImplantName NVARCHAR(255),
	ImplantDescription NVARCHAR(255)
);

ALTER TABLE ImplantCategory ADD CONSTRAINT UQ_ImplantCategory_ImplantName UNIQUE (ImplantName);

/****CREATE JOB STATUS TABLE****/

CREATE TABLE JobStatus (
	StatusId INT PRIMARY KEY IDENTITY(1,1),	--Auto-incrementing primary key
	StatusName NVARCHAR(255) NOT NULL,
	StatusDescription NVARCHAR(255)
);

ALTER TABLE JobStatus ADD CONSTRAINT UQ_JobStatus_StatusName UNIQUE (StatusName);

/****CREATE MR SAFETY STATUS TABLE****/

CREATE TABLE MRSafetyCategory (
	MRSafetyId INT PRIMARY KEY IDENTITY(1,1),
	MRSafetyName NVARCHAR(50) NOT NULL,
	MRSafetyDescription NVARCHAR(MAX)
);

ALTER TABLE MRSafetyCategory ADD CONSTRAINT UQ_MRSafetyCategory_MRSafetyName UNIQUE (MRSafetyName);

/****CREATE QUERY CONTACT METHOD TABLE****/
CREATE TABLE ContactMethod (
	ContactMethodId INT PRIMARY KEY IDENTITY(1,1),
	ContactMethodName NVARCHAR (255)
);

ALTER TABLE ContactMethod ADD CONSTRAINT UQ_ContactMethod_ContactMethodType UNIQUE (ContactMethodName);

/****CREATE JOB TABLE****/

CREATE TABLE Job (
	JobId INT PRIMARY KEY IDENTITY(1,1),
	DateJobLogged DATETIME,
	PatientId INT FOREIGN KEY REFERENCES Patient(PatientId),
	PatientTypeId INT FOREIGN KEY REFERENCES PatientType(PatientTypeId),
	UrgencyId INT FOREIGN KEY REFERENCES Urgency(UrgencyId),
	SiteId INT FOREIGN KEY REFERENCES Site(SiteId),
	DateMRIRequested DATETIME,
	DateMRIPlanned DATETIME,
	CurrentJobStatus INT,
	DateJobCompleted DATETIME
);

/****ADD ADDITIONAL FIELDS TO JOB TABLE****/

ALTER TABLE Job
ADD ImplantId INT FOREIGN KEY REFERENCES ImplantCategory(ImplantId)

ALTER TABLE Job
ADD MRSafetyId INT FOREIGN KEY REFERENCES MRSafetyCategory(MRSafetyId)

ALTER TABLE Job
ADD JobCode INT

ALTER TABLE Job
ADD FOREIGN KEY (PatientId) REFERENCES Patient(PatientId)

ALTER TABLE Job
DROP COLUMN CurrentJobStatus

ALTER TABLE Job ADD CONSTRAINT UQ_Job_JobCode UNIQUE (JobCode);

/****CREATE QUERY TABLE****/

CREATE TABLE Query (
	QueryId INT PRIMARY KEY IDENTITY(1,1),
	DateQueryReceived DATETIME,
	QueryText NVARCHAR(MAX),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	ContactMethodId INT FOREIGN KEY REFERENCES ContactMethod(ContactMethodId)
);

ALTER TABLE Query ADD CONSTRAINT UQ_Query_JobId_DateQueryReceived UNIQUE (JobId, DateQueryReceived);

/****CREATE ACTION TRACKER TABLE****/
CREATE TABLE ActionLog (
	ActionId INT PRIMARY KEY IDENTITY(1,1),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	PerformedBy INT FOREIGN KEY REFERENCES Staff(StaffId),
	ActionDescription NVARCHAR(MAX), 
	PerformedDate DATETIME,
);


/****CREATE ACTION TRACKER TABLE****/
CREATE TABLE CRISComment (
	CommentId INT PRIMARY KEY IDENTITY(1,1),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	CommentText NVARCHAR(MAX),
	CreatedDate DATETIME
);

/****CREATE XRAY CHECK TABLE****/
CREATE TABLE XRayCheck (
	CheckId INT PRIMARY KEY IDENTITY(1,1),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	StaffId INT FOREIGN KEY REFERENCES Staff(StaffId),
	CheckDate DATETIME
);

ALTER TABLE XRayCheck
ADD FOREIGN KEY (JobId) REFERENCES Job(JobId)


/****CREATE XRAY CHECK STAFF TABLE****/
CREATE TABLE XRayCheckStaff (
	XrayStaffId INT PRIMARY KEY IDENTITY(1,1),
	CheckId INT FOREIGN KEY REFERENCES XRayCheck(CheckId),
	StaffId INT FOREIGN KEY REFERENCES Staff(StaffId),
);

/**** MERGE XRAY CHECK AND XRAY CHECK STAFF TABLE FOR SIMPLICITY ****/

ALTER TABLE XRayCheck
DROP COLUMN CheckDate	--Data not reliably available

ALTER TABLE XRayCheck
ADD StaffId INT FOREIGN KEY REFERENCES Staff(StaffId)

DROP TABLE XRayCheckStaff

/****CREATE JOB STAFF TABLE****/
CREATE TABLE JobStaff (
	JobStaffId INT PRIMARY KEY IDENTITY(1,1),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	StaffId INT FOREIGN KEY REFERENCES Staff(StaffId)
);

ALTER TABLE CRISComment
ADD ActionId INT FOREIGN KEY REFERENCES ActionTracker(ActionId)

UPDATE ImplantCategory
SET ImplantName = UPPER(ImplantName);

UPDATE ImplantCategory
SET ImplantName = 'FB (FOREIGN BODY)' WHERE ImplantName = 'FB FOREIGN BODY'

UPDATE ImplantCategory
SET ImplantName = 'ORTHOPEDIC (INTERNAL)' WHERE ImplantName = 'ORTHOPAEDIC (INTERNAL)'

/****CREATE JOB STATUS HISTORY TABLE****/
CREATE TABLE StatusLog (
	JobStatusId INT PRIMARY KEY IDENTITY(1,1),
	JobId INT FOREIGN KEY REFERENCES Job(JobId),
	StatusId INT FOREIGN KEY REFERENCES JobStatus(StatusId),
	ChangedDate DATETIME,
);


/**** DROP STAGING TABLES ****/

TRUNCATE TABLE CRIS_Temp
DROP TABLE CRIS_Temp

TRUNCATE TABLE Job_Temp
DROP TABLE Job_Temp

TRUNCATE TABLE Status_Temp
DROP TABLE Status_Temp

TRUNCATE TABLE Actions_Temp
DROP TABLE Actions_Temp

-- Add CreatedDate and ModifiedDate to each table

ALTER TABLE ActionLog
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE ContactMethod
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE CRISComment
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE ImplantCategory
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Job
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE JobStaff
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE JobStatus
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE MRSafetyCategory
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Patient
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE PatientType
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Query
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Site
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Staff
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE StatusLog
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE Urgency
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

ALTER TABLE XRayCheck
ADD CreatedDate DATETIME DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME DEFAULT GETDATE() NOT NULL;

/**** CREATE INDEXES ****/
CREATE INDEX IX_Job_JobCode ON Job (JobCode);

CREATE INDEX IX_Job_UrgencyId_PatientTypeId ON Job (UrgencyId, PatientTypeId);

CREATE INDEX IX_Urgency_UrgencyCode ON Urgency (UrgencyCode)
INCLUDE (UrgencyType);

CREATE NONCLUSTERED INDEX IX_StatusLog_JobId_DateStatusChanged
ON StatusLog (JobId, ChangedDate DESC);

