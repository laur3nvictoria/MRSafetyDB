/**** NUMBER OF JOBS PER SITE ****/

SELECT
	s.SiteLongName,
	COUNT(j.JobId) AS JobsPerSite
FROM Job j
JOIN Site s ON j.SiteId = s.SiteId
GROUP BY s.SiteLongName
ORDER BY JobsPerSite DESC;

/**** NUMBER OF OVERDUE JOBS ****/

SELECT COUNT(JobCode) AS OverdueJobCount
FROM MasterSafetyWorklist_Pseudo AS JobView
WHERE PhysicsTargetDate < GETDATE() 
	AND CurrentStatus IN ('Waiting','Planned','Waiting Others');

/**** TOP 5 IMPLANT TYPES ****/

SELECT TOP 5
	i.ImplantName,
	COUNT(j.JobId) AS JobCount
FROM Job j
INNER JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
GROUP BY i.ImplantName
ORDER BY JobCount DESC;

/**** IMPLANT TYPES BY MR SAFETY CONDITION ****/

SELECT
	i.ImplantName,
	SUM (CASE WHEN j.MRSafetyId = 1 THEN 1 ELSE 0 END) AS Conditional,
	SUM (CASE WHEN j.MRSafetyId = 2 THEN 1 ELSE 0 END) AS Safe, 
	SUM (CASE WHEN j.MRSafetyId = 3 THEN 1 ELSE 0 END) AS Unsafe,
	SUM (CASE WHEN j.MRSafetyId = 4 THEN 1 ELSE 0 END) AS OffLabel, 
	SUM (CASE WHEN j.MRSafetyId = 5 THEN 1 ELSE 0 END) AS RiskAssess
FROM Job j
INNER JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
GROUP BY i.ImplantName
ORDER BY i.ImplantName;

/**** IMPLANT TYPES BY MR SAFETY CONDITION PERCENTAGE ****/

SELECT 
    i.ImplantName,
    ROUND(100 * SUM(CASE WHEN j.MRSafetyId = 1 THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN j.MRSafetyId IS NOT NULL THEN 1 ELSE 0 END), 0),0) AS ConditionalPercentage,
    ROUND(100 * SUM(CASE WHEN j.MRSafetyId = 2 THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN j.MRSafetyId IS NOT NULL THEN 1 ELSE 0 END), 0),0) AS SafePercentage,
    ROUND(100 * SUM(CASE WHEN j.MRSafetyId = 3 THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN j.MRSafetyId IS NOT NULL THEN 1 ELSE 0 END), 0),0) AS UnsafePercentage,
    ROUND(100 * SUM(CASE WHEN j.MRSafetyId = 4 THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN j.MRSafetyId IS NOT NULL THEN 1 ELSE 0 END), 0),0) AS OffLabelPercentage,
    ROUND(100 * SUM(CASE WHEN j.MRSafetyId = 5 THEN 1 ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN j.MRSafetyId IS NOT NULL THEN 1 ELSE 0 END), 0),0) AS RiskAssessPercentage
FROM 
    Job j
INNER JOIN 
    ImplantCategory i ON j.ImplantId = i.ImplantId
GROUP BY 
    i.ImplantName
ORDER BY 
    i.ImplantName;

-- Number of overdue jobs for inpatients vs outpatients --
SELECT
	v.PatientTypeName,
	COUNT(v.JobCode) AS OverdueJobCount,
	AVG(DATEDIFF(DAY,v.PhysicsTargetDate, GETDATE())) AS AvgOverdueDays
FROM MasterSafetyWorklist_Pseudo v
WHERE v.PhysicsTargetDate < GETDATE() AND CurrentStatus IN ('Waiting','Planned','Physics Done','Waiting Others')
GROUP BY v.PatientTypeName
ORDER BY OverdueJobCount DESC;

-- Jobs which were completed within the turnaround time based on urgency & patient type (e.g. outpatient 2ww in 48 hours) --
WITH CompletedJobs AS (
SELECT
	JobCode, 
	UrgencyCode,
	UrgencyType,
	PatientTypeName AS PatientType,
	CASE
		WHEN DateStatusChanged <= PhysicsTargetDate THEN 1
		ELSE 0
	END AS TurnaroundMet
FROM MasterSafetyWorklist_Pseudo
WHERE CurrentStatus IN ('Complete','Rejected','Not Requested') AND DateStatusChanged IS NOT NULL
)
SELECT 
	cj.UrgencyCode,
	cj.UrgencyType,
	cj.PatientType,
	COUNT(cj.JobCode) AS TotalJobsCompleted, 
	SUM(cj.TurnaroundMet) AS JobsMetTurnaround, 
	ROUND((SUM(cj.TurnaroundMet)*100.0 / COUNT(cj.JobCode)),2) AS PercentageMeetingTurnaround
FROM CompletedJobs cj
GROUP BY cj.UrgencyCode, cj.UrgencyType, cj.PatientType
ORDER BY cj.UrgencyType, cj.PatientType

/**** QUERIES BY CONTACT METHOD WITH AVERAGE TIME TO LOG (IN DAYS) ****/

SELECT
	ContactMethodName,
	COUNT(JobId) AS QueryCount,
	AVG(DATEDIFF(DAY, DateQueryReceived, DateJobLogged)) AS AvgTimeToLog
FROM MasterSafetyWorklist_Pseudo
WHERE ContactMethodName IS NOT NULL
	AND DateQueryReceived IS NOT NULL
	AND DateJobLogged IS NOT NULL
GROUP BY ContactMethodName
ORDER BY ContactMethodName

