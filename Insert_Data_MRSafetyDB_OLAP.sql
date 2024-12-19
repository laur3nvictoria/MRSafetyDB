/**** POPULATE DIMDATE ****/

-- Declare the date range for generating the DimDate table
DECLARE @StartDate DATE = '2020-01-01'; -- Start date for the range
DECLARE @EndDate DATE = '2030-12-31';   -- End date for the range

-- Iterate through each date in the range
WHILE @StartDate <= @EndDate
BEGIN
	-- Insert a new row into the DimDate table for each date
    INSERT INTO DimDate (
        DateKey,		-- A unique integer key for each date (format: YYYYMMDD)
        FullDate,		-- The full date value
        Year,			-- The year component of the date
        Quarter,		-- The quarter of the year (1-4)
        Month,			-- The month component of the date (1-12)
		Week,			-- The week number in the year
        MonthName,		-- The full name of the month
		DayOfMonth,		-- The day of the month (1-31)
        DayOfWeek,		-- The day of the week (1=Sunday, 7=Saturday)
        DayName,		-- The full name of the day
        IsWeekend		-- A flag indicating if the day is a weekend (1=Yes, 0=No)
    )
    SELECT 
        CONVERT(INT, FORMAT(@StartDate, 'yyyyMMdd')) AS DateKey,	-- Format the date as YYYYMMDD
        @StartDate AS FullDate,
        YEAR(@StartDate) AS Year,
        DATEPART(QUARTER, @StartDate) AS Quarter,
        MONTH(@StartDate) AS Month,
		DATEPART(WEEK, @StartDate) AS Week,
        DATENAME(MONTH, @StartDate) AS MonthName,
        DAY(@StartDate) AS DayOfMonth,
        DATEPART(WEEKDAY, @StartDate) AS DayOfWeek,
        DATENAME(WEEKDAY, @StartDate) AS DayName,
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend;

    -- Increment the date by 1 day
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;

/***ETL STAFF TABLE***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Staff table into the OLAP DimStaff table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimStaff AS Target
USING (
	SELECT StaffId, Initials, FirstName, LastName
	FROM CMI_MRSafetyDB.dbo.Staff
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.StaffId = Source.StaffId		-- Match on the StaffId
WHEN MATCHED THEN
	UPDATE SET
		Target.Initials = Source.Initials,
		Target.FirstName = Source.FirstName,
		Target.LastName = Source.LastName		-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (StaffId, Initials, FirstName, LastName)		-- Insert new records if not matched
	VALUES (Source.StaffId, Source.Initials, Source.FirstName, Source.LastName);

/***ETL IMPLANT TABLE***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

-- Merge the source data from the OLTP ImplantCategory table into the OLAP DimImplant table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimImplant AS Target
USING (
	SELECT ImplantId, ImplantName, ImplantDescription
	FROM CMI_MRSafetyDB.dbo.ImplantCategory
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.ImplantId = Source.ImplantId		-- Match on the ImplantId
WHEN MATCHED THEN
	UPDATE SET
		Target.ImplantName = Source.ImplantName,
		Target.ImplantDescription = Source.ImplantDescription	-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (ImplantId, ImplantName, ImplantDescription)			-- Insert new records if not matched
	VALUES (Source.ImplantId, Source.ImplantName, Source.ImplantDescription);

/***ETL URGENCY TABLE***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Urgency table into the OLAP DimUrgency table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimUrgency AS Target
USING (
	SELECT UrgencyId, UrgencyCode, UrgencyType, UrgencyDescription
	FROM CMI_MRSafetyDB.dbo.Urgency
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.UrgencyId = Source.UrgencyId		-- Match on the UrgencyId
WHEN MATCHED THEN
	UPDATE SET
		Target.UrgencyCode = Source.UrgencyCode,
		Target.UrgencyType = Source.UrgencyType,
		Target.UrgencyDescription = Source.UrgencyDescription	-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (UrgencyId, UrgencyCode, UrgencyType, UrgencyDescription)	-- Insert new records if not matched
	VALUES (Source.UrgencyId, Source.UrgencyCode, Source.UrgencyType, Source.UrgencyDescription);

/***ETL SITE TABLE***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Site table into the OLAP DimSite table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimSite AS Target
USING (
	SELECT SiteId, SiteShortName, SiteLongName
	FROM CMI_MRSafetyDB.dbo.Site
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.SiteId = Source.SiteId		-- Match on the SiteId
WHEN MATCHED THEN
	UPDATE SET
		Target.SiteShortName = Source.SiteShortName,
		Target.SiteLongName = Source.SiteLongName		-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (SiteId, SiteShortName, SiteLongName)		-- Insert new records if not matched
	VALUES (Source.SiteId, Source.SiteShortName, Source.SiteLongName);

/***ETL PATIENT TYPE TABLE***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Urgency table into the OLAP DimUrgency table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimPatientType AS Target
USING (
	SELECT PatientTypeId, PatientTypeName, PatientTypeCode
	FROM CMI_MRSafetyDB.dbo.PatientType
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.PatientTypeId = Source.PatientTypeId		-- Match on the UrgencyId
WHEN MATCHED THEN
	UPDATE SET
		Target.PatientTypeName = Source.PatientTypeName,
		Target.PatientTypeCode = Source.PatientTypeCode		-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (PatientTypeId, PatientTypeName, PatientTypeCode)	-- Insert new records if not matched
	VALUES (Source.PatientTypeId, Source.PatientTypeName, Source.PatientTypeCode);

/***ETL CONTACT METHOD TABLE ***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE()); -- Example start date

DECLARE @LastETLRunTime DATETIME = '2024-01-01 00:00:00'; -- Set to your specific date and time


-- Merge the source data from the OLTP Urgency table into the OLAP DimUrgency table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimContactMethod AS Target
USING (
	SELECT ContactMethodId, ContactMethodName
	FROM CMI_MRSafetyDB.dbo.ContactMethod
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.ContactMethodId = Source.ContactMethodId		-- Match on the UrgencyId
WHEN MATCHED THEN
	UPDATE SET
		Target.ContactMethodName = Source.ContactMethodName    -- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (ContactMethodId, ContactMethodName)	-- Insert new records if not matched
	VALUES (Source.ContactMethodId, Source.ContactMethodName);

/*** ETL JOB STATUS TABLE ***/

DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -30, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Urgency table into the OLAP DimUrgency table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimJobStatus AS Target
USING (
	SELECT StatusId, StatusName, StatusDescription
	FROM CMI_MRSafetyDB.dbo.JobStatus
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.StatusId = Source.StatusId		-- Match on the UrgencyId
WHEN MATCHED THEN
	UPDATE SET
		Target.StatusName = Source.StatusName,
		Target.StatusDescription = Source.StatusDescription		-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (StatusId, StatusName, StatusDescription)	-- Insert new records if not matched
	VALUES (Source.StatusId, Source.StatusName, Source.StatusDescription);

/*** ETL MR SAFETY TABLE ***/
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -30, GETDATE()); -- Example start date

-- Merge the source data from the OLTP Urgency table into the OLAP DimUrgency table
MERGE CMI_MRSafetyDB_OLAP.dbo.DimMRSafety AS Target
USING (
	SELECT MRSafetyId, MRSafetyName, MRSafetyDescription
	FROM CMI_MRSafetyDB.dbo.MRSafetyCategory
	WHERE ModifiedDate >= @LastETLRunTime	-- Filter for records modified since the last ETL runtime
) AS SOURCE
ON Target.SafetyId = Source.MRSafetyId		-- Match on the UrgencyId
WHEN MATCHED THEN
	UPDATE SET
		Target.SafetyName = Source.MRSafetyName,
		Target.SafetyDescription = Source.MRSafetyDescription		-- Update existing records if matched
WHEN NOT MATCHED THEN
	INSERT (SafetyId, SafetyName, SafetyDescription)	-- Insert new records if not matched
	VALUES (Source.MRSafetyId, Source.MRSafetyName, Source.MRSafetyDescription);

/*** ETL JOB TABLE ***/

-- Declare the last ETL runtime for filtering modified data
DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -30, GETDATE());

-- Merge the source data from the OLTP Job table into the OLAP FactJob table

MERGE CMI_MRSafetyDB_OLAP.dbo.FactJob AS Target
USING (
	SELECT
		j.JobId,		-- Primary key for the job
		j.JobCode,
		ddLogged.DateKey AS DateJobLoggedKey,
		ddQuery.DateKey AS DateQueryReceivedKey,
		du.UrgencyId AS UrgencyKey,		-- Foreign key for DimUrgency
		pt.PatientTypeId AS PatientTypeKey,
		s.SiteId AS SiteKey,			-- Foreign key for DimSite
		ddMRIRequested.DateKey AS DateMRIRequestedKey,
		ddMRIPlanned.DateKey AS DateMRIPlannedKey,
		j.ImplantId AS ImplantKey,		-- Foreign key for DimImplant
		j.MRSafetyId AS MRSafetyKey,
		q.ContactMethodId AS ContactMethodKey,

		-- Current Status Id --
		( 
			SELECT TOP 1 sl.StatusId
			FROM CMI_MRSafetyDB.dbo.StatusLog sl
			--JOIN CMI_MRSafetyDB.dbo.JobStatus jst ON sl.StatusId = jst.StatusId
			WHERE sl.JobId = j.JobId
			ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
		) AS CurrentStatusKey,

		-- Date Current Status Key --
		(
			SELECT TOP 1 dd.DateKey
			FROM CMI_MRSafetyDB.dbo.StatusLog sl
			JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate dd ON dd.FullDate = CAST(sl.ChangedDate AS DATE)
			WHERE sl.JobId = j.JobId
			ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
		) AS DateCurrentStatusKey,

		-- PTL Target Date Mapping --
		ddPTLTarget.DateKey AS PTLTargetDateKey,

		-- Physics Target Date Mapping --
		ddPhysicsTarget.DateKey AS PhysicsTargetDateKey

	FROM CMI_MRSafetyDB.dbo.Job j
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimUrgency du ON j.UrgencyId = du.UrgencyId
	LEFT JOIN CMI_MRSafetyDB.dbo.PatientType pt ON j.PatientTypeId = pt.PatientTypeId
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimSite s ON j.SiteId = s.SiteId
	LEFT JOIN CMI_MRSafetyDB.dbo.ImplantCategory i ON j.ImplantId = i.ImplantId
	LEFT JOIN CMI_MRSafetyDB.dbo.MRSafetyCategory mr ON j.MRSafetyId = mr.MRSafetyId
	LEFT JOIN CMI_MRSafetyDB.dbo.Query q ON j.JobId = q.JobId
	LEFT JOIN CMI_MRSafetyDB.dbo.ContactMethod cm ON cm.ContactMethodId = q.ContactMethodId
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddLogged ON ddLogged.FullDate = CAST(j.DateJobLogged AS DATE)
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddQuery ON ddQuery.FullDate = CAST(q.DateQueryReceived AS DATE)
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddMRIRequested ON ddMRIRequested.FullDate = CAST(j.DateMRIRequested AS DATE)
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddMRIPlanned ON ddMRIPlanned.FullDate = CAST(j.DateMRIPlanned AS DATE)

	-- PTL Target Date Mapping --
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddPTLTarget ON ddPTLTarget.FullDate = CASE
		WHEN du.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
		WHEN du.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, j.DateMRIRequested)
		WHEN du.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
		WHEN du.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, j.DateMRIRequested)
		WHEN du.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
		WHEN du.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, j.DateMRIRequested)
		WHEN du.UrgencyCode = 7 THEN DATEADD(DAY, 2, j.DateMRIRequested)
		WHEN du.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
		WHEN du.UrgencyCode = 15 THEN DATEADD(DAY, 2, j.DateMRIRequested)
		ELSE DATEADD(DAY, 14, j.DateMRIRequested)
	END

	-- Physics Target Date Mapping --
	LEFT JOIN CMI_MRSafetyDB_OLAP.dbo.DimDate ddPhysicsTarget ON ddPhysicsTarget.FullDate = CASE
		WHEN du.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
		WHEN du.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, q.DateQueryReceived)
		WHEN du.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
		WHEN du.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, q.DateQueryReceived)
		WHEN du.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
		WHEN du.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, q.DateQueryReceived)
		WHEN du.UrgencyCode = 7 THEN DATEADD(DAY, 2, q.DateQueryReceived)
		WHEN du.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
		WHEN du.UrgencyCode = 15 THEN DATEADD(DAY, 2, q.DateQueryReceived)
		ELSE DATEADD(DAY, 14, q.DateQueryReceived)
	END
) AS Source
ON Target.JobId = Source.JobId

WHEN MATCHED THEN
	UPDATE SET
		Target.JobCode = Source.JobCode,
		Target.DateJobLoggedKey = Source.DateJobLoggedKey,
		Target.DateQueryReceivedKey = Source.DateQueryReceivedKey,
		Target.UrgencyKey = Source.UrgencyKey,
		Target.PatientTypeKey = Source.PatientTypeKey,
		Target.SiteKey = Source.SiteKey,
		Target.DateMRIRequestedKey = Source.DateMRIRequestedKey,
		Target.DateMRIPlannedKey = Source.DateMRIPlannedKey,
		Target.ImplantKey = Source.ImplantKey,
		Target.MRSafetyKey = Source.MRSafetyKey,
		Target.ContactMethodKey = Source.ContactMethodKey,
		Target.CurrentStatusKey = Source.CurrentStatusKey,
		Target.DateCurrentStatusKey = Source.DateCurrentStatusKey,
		Target.PTLTargetDateKey = Source.PTLTargetDateKey,
		Target.PhysicsTargetDateKey = Source.PhysicsTargetDateKey

WHEN NOT MATCHED THEN
	INSERT (
		JobId, 
		JobCode, 
		DateJobLoggedKey, 
		DateQueryReceivedKey, 
		UrgencyKey, 
		PatientTypeKey, 
		SiteKey, 
		DateMRIRequestedKey, 
		DateMRIPlannedKey, 
		ImplantKey, 
		MRSafetyKey, 
		ContactMethodKey, 
		CurrentStatusKey, 
		DateCurrentStatusKey, 
		PTLTargetDateKey, 
		PhysicsTargetDateKey
	)
	VALUES (
		Source.JobId,
		Source.JobCode,
		Source.DateJobLoggedKey,
		Source.DateQueryReceivedKey,
		Source.UrgencyKey,
		Source.PatientTypeKey,
		Source.SiteKey,
		Source.DateMRIRequestedKey,
		Source.DateMRIPlannedKey,
		Source.ImplantKey,
		Source.MRSafetyKey,
		Source.ContactMethodKey,
		Source.CurrentStatusKey,
		Source.DateCurrentStatusKey,
		Source.PTLTargetDateKey,
		Source.PhysicsTargetDateKey
	);

DECLARE @LastETLRunTime DATETIME = DATEADD(DAY, -7, GETDATE());

MERGE INTO CMI_MRSafetyDB_OLAP.dbo.BridgeStaffAssigned AS Target
USING (
    -- Combine JobStaff and XRayCheck data from OLTPDatabase
    SELECT 
        f.JobKey,
        js.StaffId AS StaffKey,
        CASE 
            WHEN x.StaffId IS NOT NULL THEN 1
            ELSE 0
        END AS XRayCheck
    FROM CMI_MRSafetyDB_OLAP.dbo.FactJob f

    INNER JOIN CMI_MRSafetyDB.dbo.JobStaff js ON f.JobID = js.JobID -- Match staff assigned to jobs
    LEFT JOIN CMI_MRSafetyDB.dbo.XRayCheck x 
        ON f.JobID = x.JobID AND js.StaffId = x.StaffId -- Check for XRay participation
		WHERE js.ModifiedDate >= @LastETLRunTime
) AS Source
ON Target.JobKey = Source.JobKey AND Target.StaffKey = Source.StaffKey

-- UPDATE existing records in the bridge table
WHEN MATCHED THEN
    UPDATE SET 
        Target.XRayCheck = Source.XRayCheck

-- INSERT new records into the bridge table
WHEN NOT MATCHED THEN
    INSERT (JobKey, StaffKey, XRayCheck)
    VALUES (Source.JobKey, Source.StaffKey, Source.XRayCheck);


