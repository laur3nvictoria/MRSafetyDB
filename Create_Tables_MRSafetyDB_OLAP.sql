/****CREATE JOB FACT ****/


CREATE TABLE FactJob (
    JobKey INT IDENTITY(1,1) PRIMARY KEY,
    JobId INT NOT NULL,
	JobCode INT NOT NULL,
	DateJobLoggedKey INT,
	DateQueryReceivedKey INT,
    UrgencyKey INT,
    PatientType NVARCHAR(50),
    SiteKey INT,
	DateMRIRequestedKey INT,
    DateMRIPlannedKey INT,
    ImplantKey INT,
    MRSafety NVARCHAR(50),
    ContactMethod NVARCHAR(50),
    CurrentStatusKey INT,
	DateCurrentStatusKey INT,
    PTLTargetDateKey INT,
    PhysicsTargetDateKey INT
);

ALTER TABLE FactJob
ADD CurrentStatusKey INT

ALTER TABLE FactJob
ADD StaffAssigned NVARCHAR(50)

ALTER TABLE FactJob
ADD XRayCheckedBy NVARCHAR(50)

ALTER TABLE FactJob
ADD PatientTypeKey INT

ALTER TABLE FactJob
DROP COLUMN XRayCheckedBy

ALTER TABLE FactJob
DROP COLUMN ContactMethod

ALTER TABLE FactJob
ADD ContactMethodKey INT

ALTER TABLE FactJob
DROP COLUMN StaffAssigned

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimUrgency
FOREIGN KEY (UrgencyKey) REFERENCES DimUrgency(UrgencyKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimImplant
FOREIGN KEY (ImplantKey) REFERENCES DimImplant(ImplantKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimDate_DateJobLogged
FOREIGN KEY (DateJobLoggedKey) REFERENCES DimDate(DateKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimDate_DateMRIRequested
FOREIGN KEY (DateMRIRequestedKey) REFERENCES DimDate(DateKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimDate_DateMRIPlanned
FOREIGN KEY (DateMRIPlannedKey) REFERENCES DimDate(DateKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimDate_DateQueryReceived
FOREIGN KEY (DateQueryReceivedKey) REFERENCES DimDate(DateKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimPatientType
FOREIGN KEY (PatientTypeKey) REFERENCES DimPatientType(PatientTypeKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimMRSafety
FOREIGN KEY (MRSafetyKey) REFERENCES DimMRSafety(SafetyKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimContactMethod
FOREIGN KEY (ContactMethodKey) REFERENCES DimContactMethod(ContactMethodKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimJobStatus
FOREIGN KEY (CurrentStatusKey) REFERENCES DimJobStatus(StatusKey)

ALTER TABLE FactJob
WITH NOCHECK
ADD CONSTRAINT FK_FactJob_DimSite
FOREIGN KEY (SiteKey) REFERENCES DimSite(SiteKey)

ALTER TABLE FactJob
DROP COLUMN IsOverdue

/****CREATE URGENCY DIMENSION TABLE ****/

CREATE TABLE DimUrgency (
    UrgencyKey INT IDENTITY(1,1) PRIMARY KEY,
    UrgencyId INT NOT NULL,
    UrgencyCode INT NOT NULL,
    UrgencyType NVARCHAR(50),
    UrgencyDescription NVARCHAR(255)
);

/****CREATE PATIENT TYPE DIMENSION TABLE - NOW DELETED ****/

CREATE TABLE DimPatientType (
    PatientTypeKey INT IDENTITY(1,1) PRIMARY KEY,
    PatientTypeId INT NOT NULL,
    PatientTypeName NVARCHAR(50),
    PatientTypeCode NVARCHAR(10)
);

/****CREATE SITE DIMENSION TABLE ****/

CREATE TABLE DimSite (
    SiteKey INT IDENTITY(1,1) PRIMARY KEY,
    SiteId INT NOT NULL,
    SiteShortName NVARCHAR(50),
    SiteLongName NVARCHAR(255)
);

/****CREATE IMPLANT DIMENSION TABLE ****/

CREATE TABLE DimImplant (
    ImplantKey INT IDENTITY(1,1) PRIMARY KEY,
    ImplantId INT NOT NULL,
    ImplantName NVARCHAR(255),
    ImplantDescription NVARCHAR(255)
);

ALTER TABLE DimImplant
ADD JobCount INT


/****CREATE MR SAFETY DIMENSION TABLE - NOW DELETED ****/

CREATE TABLE DimMRSafety (
    SafetyKey INT IDENTITY(1,1) PRIMARY KEY,
    SafetyId INT NOT NULL,
    SafetyName NVARCHAR(50),
    SafetyDescription NVARCHAR(255)
);

/****CREATE CONTACT METHOD DIMENSION TABLE - NOW DELETED ****/

CREATE TABLE DimContactMethod (
    ContactMethodKey INT IDENTITY(1,1) PRIMARY KEY,
    ContactMethodId INT NOT NULL,
    ContactMethodName NVARCHAR(255)
);

/****CREATE JOB STATUS DIMENSION TABLE ****/

CREATE TABLE DimJobStatus (
    StatusKey INT IDENTITY(1,1) PRIMARY KEY,
    StatusId INT NOT NULL,
    StatusName NVARCHAR(255),
    StatusDescription NVARCHAR(255)
);

/***CREATE DATE DIMENSION TABLE****/

CREATE TABLE DimDate (
    DateKey INT IDENTITY(1,1) PRIMARY KEY,
    FullDate DATETIME NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
	MonthName NVARCHAR(20) NOT NULL,
    DayOfMonth INT NOT NULL,
	DayOfWeek INT NOT NULL,
	DayName NVARCHAR(20) NOT NULL,
	IsWeekend BIT NOT NULL,
    Quarter INT NOT NULL
);

/****CREATE STAFF DIMENSION TABLE****/

CREATE TABLE DimStaff (
	StaffKey INT IDENTITY(1,1) PRIMARY KEY,
	StaffId INT NOT NULL,
	FirstName NVARCHAR(255),
	LastName NVARCHAR(255),
	Initials NVARCHAR(10)
);

ALTER TABLE DimDate
ADD Week INT NOT NULL

DROP TABLE DimMRSafety
DROP TABLE DimPatientType
DROP TABLE DimContactMethod

/**** CREATE BRIDGE STAFF ASSIGNED TABLE ****/

CREATE TABLE BridgeStaffAssigned (
	StaffAssignedKey INT IDENTITY (1,1) PRIMARY KEY,
	JobKey INT, 
	StaffKey INT,
	XRayCheck BIT
	);

ALTER TABLE BridgeStaffAssigned
WITH NOCHECK
ADD CONSTRAINT FK_BridgeStaffAssigned_DimStaff
FOREIGN KEY (StaffKey) REFERENCES DimStaff(StaffKey)

ALTER TABLE BridgeStaffAssigned
WITH NOCHECK
ADD CONSTRAINT FK_BridgeStaffAssigned_FactJob
FOREIGN KEY (JobKey) REFERENCES FactJob(JobKey)