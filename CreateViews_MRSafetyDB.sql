
CREATE OR ALTER VIEW [dbo].[MasterSafetyWorklist_Pseudo] AS
SELECT 
    j.JobId,
	j.JobCode,
    q.DateQueryReceived,
    j.DateJobLogged,
    cm.ContactMethodName,
    j.PatientId,
    s.SiteShortName,
    pt.PatientTypeName,
    u.UrgencyCode,
    u.UrgencyType,
    j.DateMRIRequested,
    j.DateMRIPlanned,
    
    -- PTL TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, j.DateMRIRequested)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, j.DateMRIRequested)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, j.DateMRIRequested)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        ELSE DATEADD(DAY, 14, j.DateMRIRequested)
    END AS PTLTargetDate,

    -- PHYSICS TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, q.DateQueryReceived)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, q.DateQueryReceived)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, q.DateQueryReceived)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        ELSE DATEADD(DAY, 14, q.DateQueryReceived)
    END AS PhysicsTargetDate,

    i.ImplantName,
    mr.MRSafetyName,
    
    -- STAFF ASSIGNED --
    (
        SELECT STUFF((
            SELECT '/' + sf.Initials
            FROM JobStaff js
            LEFT JOIN Staff sf ON js.StaffId = sf.StaffId
            WHERE js.JobId = j.JobId
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
    ) AS StaffAssigned,

    -- CURRENT STATUS --
    (
        SELECT TOP 1 jst.StatusName
        FROM StatusLog sl
        JOIN JobStatus jst ON sl.StatusId = jst.StatusId
        WHERE sl.JobId = j.JobId
        ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
    ) AS CurrentStatus,

    -- CURRENT STATUS DATE --
    (
        SELECT TOP 1 sl.ChangedDate
        FROM StatusLog sl
        WHERE sl.JobId = j.JobId
        ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
    ) AS DateStatusChanged,

		-- X RAY CHECK --
	    (
        SELECT STUFF((
            SELECT '/' + sf.Initials
            FROM XRayCheck xc
            LEFT JOIN Staff sf ON xc.StaffId = sf.StaffId
            WHERE xc.JobId = j.JobId
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
    ) AS XRayCheckPerfomedBy

FROM Job j
JOIN Urgency u ON j.UrgencyId = u.UrgencyId
LEFT JOIN Query q ON j.JobId = q.JobId
LEFT JOIN ContactMethod cm ON q.ContactMethodId = cm.ContactMethodId
LEFT JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
LEFT JOIN Site s ON j.SiteId = s.SiteId
LEFT JOIN MRSafetyCategory mr ON j.MRSafetyId = mr.MRSafetyId
LEFT JOIN PatientType pt ON j.PatientTypeId = pt.PatientTypeId

GROUP BY 
    j.JobCode, 
	j.JobId,
    q.DateQueryReceived, 
    j.DateJobLogged, 
    cm.ContactMethodName, 
    j.PatientId, 
    s.SiteShortName, 
    pt.PatientTypeName, 
    u.UrgencyCode, 
    u.UrgencyType, 
	j.DateMRIRequested,
    j.DateMRIPlanned,
    i.ImplantName, 
    mr.MRSafetyName;
GO


CREATE OR ALTER VIEW [dbo].[MasterSafetyWorklist_Full] AS
SELECT 
    j.JobId,
	j.JobCode,
    q.DateQueryReceived,
    j.DateJobLogged,
    cm.ContactMethodName,
    p.PatientCode,
	p.Initials,
    s.SiteShortName,
    pt.PatientTypeName,
    u.UrgencyCode,
    u.UrgencyType,
    j.DateMRIRequested,
    j.DateMRIPlanned,
    
    -- PTL TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, j.DateMRIRequested)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, j.DateMRIRequested)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, j.DateMRIRequested)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        ELSE DATEADD(DAY, 14, j.DateMRIRequested)
    END AS PTLTargetDate,

    -- PHYSICS TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, q.DateQueryReceived)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, q.DateQueryReceived)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, q.DateQueryReceived)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        ELSE DATEADD(DAY, 14, q.DateQueryReceived)
    END AS PhysicsTargetDate,
	q.QueryText,
	
	-- LAST ACTION --
	(
		SELECT TOP 1 al.ActionDescription
		FROM ActionLog al
		WHERE al.JobId = j.JobId
		ORDER BY al.PerformedDate DESC, al.ActionId DESC
	) AS LastAction,

    i.ImplantName,
    mr.MRSafetyName,
	c.Comment,
    
    -- STAFF ASSIGNED --
    (
        SELECT STUFF((
            SELECT '/' + sf.Initials
            FROM JobStaff js
            LEFT JOIN Staff sf ON js.StaffId = sf.StaffId
            WHERE js.JobId = j.JobId
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
    ) AS StaffAssigned,

    -- CURRENT STATUS --
    (
        SELECT TOP 1 jst.StatusName
        FROM StatusLog sl
        JOIN JobStatus jst ON sl.StatusId = jst.StatusId
        WHERE sl.JobId = j.JobId
        ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
    ) AS CurrentStatus,

    -- CURRENT STATUS DATE --
    (
        SELECT TOP 1 sl.ChangedDate
        FROM StatusLog sl
        WHERE sl.JobId = j.JobId
        ORDER BY sl.ChangedDate DESC, sl.JobStatusId DESC
    ) AS DateStatusChanged,

	-- X RAY CHECK --
	    (
        SELECT STUFF((
            SELECT '/' + sf.Initials
            FROM XRayCheck xc
            LEFT JOIN Staff sf ON xc.StaffId = sf.StaffId
            WHERE xc.JobId = j.JobId
            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
    ) AS XRayCheckPerfomedBy
FROM Job j
JOIN Urgency u ON j.UrgencyId = u.UrgencyId
LEFT JOIN Query q ON j.JobId = q.JobId
LEFT JOIN ContactMethod cm ON q.ContactMethodId = cm.ContactMethodId
LEFT JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
LEFT JOIN Site s ON j.SiteId = s.SiteId
LEFT JOIN MRSafetyCategory mr ON j.MRSafetyId = mr.MRSafetyId
LEFT JOIN PatientType pt ON j.PatientTypeId = pt.PatientTypeId
LEFT JOIN Patient p ON j.PatientId = p.PatientId
LEFT JOIN CRISComment c ON j.JobId = c.JobId

GROUP BY 
    j.JobCode, 
	j.JobId,
    q.DateQueryReceived, 
    j.DateJobLogged, 
    cm.ContactMethodName, 
    p.PatientCode, 
	p.Initials,
    s.SiteShortName, 
    pt.PatientTypeName, 
    u.UrgencyCode, 
    u.UrgencyType, 
	j.DateMRIRequested,
    j.DateMRIPlanned,
	q.QueryText,
    i.ImplantName, 
    mr.MRSafetyName,
	c.Comment;
GO

CREATE OR ALTER VIEW JobsInProgress_Pseudo AS
WITH CurrentStatusCTE AS (
    SELECT 
        sl.JobId,
        jst.StatusName AS CurrentStatus,
        sl.ChangedDate AS DateStatusChanged
    FROM StatusLog sl
    JOIN JobStatus jst ON sl.StatusId = jst.StatusId
    WHERE sl.ChangedDate = (
        SELECT MAX(sl2.ChangedDate)
        FROM StatusLog sl2
        WHERE sl2.JobId = sl.JobId
    )
)
SELECT 
    j.JobCode,
    q.DateQueryReceived,
    j.DateJobLogged,
    cm.ContactMethodName,
    j.PatientId,
    s.SiteShortName,
    pt.PatientTypeName,
    u.UrgencyCode,
    u.UrgencyType,
    j.DateMRIRequested,
    j.DateMRIPlanned,
    
    -- PTL TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, j.DateMRIRequested)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, j.DateMRIRequested)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN j.DateMRIRequested
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, j.DateMRIRequested)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, j.DateMRIRequested)
        ELSE DATEADD(DAY, 14, j.DateMRIRequested)
    END AS PTLTargetDate,

    -- PHYSICS TARGET DATE --
    CASE
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 1 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 28, q.DateQueryReceived)
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 3 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 14, q.DateQueryReceived)
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Inpatient' THEN q.DateQueryReceived
        WHEN u.UrgencyCode = 5 AND pt.PatientTypeName = 'Outpatient' THEN DATEADD(DAY, 7, q.DateQueryReceived)
        WHEN u.UrgencyCode = 7 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        WHEN u.UrgencyCode = 9 THEN DATEADD(DAY, -28, j.DateMRIPlanned)
        WHEN u.UrgencyCode = 15 THEN DATEADD(DAY, 2, q.DateQueryReceived)
        ELSE DATEADD(DAY, 14, q.DateQueryReceived)
    END AS PhysicsTargetDate,

    i.ImplantName,
    mr.MRSafetyName,
    
    -- STAFF ASSIGNED --
    (
        SELECT STUFF((SELECT '/' + sf.Initials
                      FROM JobStaff js
                      LEFT JOIN Staff sf ON js.StaffId = sf.StaffId
                      WHERE js.JobId = j.JobId
                      FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
    ) AS StaffAssigned,

    cs.CurrentStatus,
    cs.DateStatusChanged
FROM Job j
JOIN Urgency u ON j.UrgencyId = u.UrgencyId
LEFT JOIN Query q ON j.JobId = q.JobId
LEFT JOIN ContactMethod cm ON q.ContactMethodId = cm.ContactMethodId
LEFT JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
LEFT JOIN Site s ON j.SiteId = s.SiteId
LEFT JOIN MRSafetyCategory mr ON j.MRSafetyId = mr.MRSafetyId
LEFT JOIN PatientType pt ON j.PatientTypeId = pt.PatientTypeId
LEFT JOIN CurrentStatusCTE cs ON j.JobId = cs.JobId
WHERE cs.CurrentStatus IN ('Waiting', 'Planned', 'Waiting Others')
GROUP BY 
    j.JobCode, 
	j.JobId,
    q.DateQueryReceived, 
    j.DateJobLogged, 
    cm.ContactMethodName, 
    j.PatientId, 
    s.SiteShortName, 
    pt.PatientTypeName, 
    u.UrgencyCode, 
    u.UrgencyType, 
	j.DateMRIRequested,
    j.DateMRIPlanned,
    i.ImplantName, 
    mr.MRSafetyName,
	cs.CurrentStatus,
	cs.DateStatusChanged;
GO

DROP VIEW MasterSafetyWorklist_Full
DROP VIEW MasterSafetyWorklist_Pseudo