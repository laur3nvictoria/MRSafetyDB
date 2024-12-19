USE CMI_MRSafetyDB
GO

/****INSERT DATA INTO PATIENT TYPE TABLE****/

INSERT INTO PatientType (PatientTypeName)
VALUES
('Inpatient'),
('Outpatient'),
('Not Specified');

UPDATE PatientType
SET PatientTypeCode = CASE
	WHEN PatientTypeName = 'Inpatient' THEN 'IN'
	WHEN PatientTypeName = 'Outpatient' THEN 'OUT'
	WHEN PatientTypeName = 'Not Specified' THEN 'NS'
END;

/****INSERT DATA INTO URGENCY TABLE****/

INSERT INTO Urgency (UrgencyCode, UrgencyType, UrgencyDescription)
VALUES
('1','Routine','Non-urgent request'),
('3','ASAP','As soon as possible'),
('5','Urgent','Urgent request'),
('7','2 Week Wait','Patient referred for MRI with suspected cancer'),
('8','Appointment','Patient has MRI booked'),
('9','Planned','Future MRI booked'),
('11','Attended','Patient in department'),
('13','On the table','Patient in MRI scanner'),
('15','Pre-request','Query from referrer before MRI requested');

INSERT INTO Urgency(UrgencyCode, UrgencyType, UrgencyDescription)
VALUES
('0','Not Specified','Urgency not specified')

/****INSERT DATA INTO SITE TABLE****/

INSERT INTO Site (SiteShortName, SiteLongName)
VALUES
('UHPT','University Hospitals Plymouth NHS Trust'),
('Exeter','Royal Devon University Healthcare NHS Foundation Trust'),
('Bristol','University Hospitals Bristol and Weston NHS Foundation Trust'),
('Truro','Royal Cornwall Hospitals NHS Trust'),
('Torbay','Torbay and South Devon NHS Foundation Trust'),
('InHealth-UHP','InHealth Plymouth MRI Unit'),
('North Devon','North Devon District Hospital'),
('BRIC','Brain Research & Imaging Centre'),
('medneo','medneo Mobile MRI service');

UPDATE Site
SET SiteShortName = UPPER(SiteShortName)

INSERT INTO Site (SiteShortName, SiteLongName)
VALUES
('Not Specified','No site details provided');

UPDATE Site
SET SiteShortName = 'INHEALTH' WHERE SiteShortName = 'INHEALTH-UHP'

/****INSERT DATA INTO CONTACT METHOD TABLE****/

INSERT INTO ContactMethod (ContactMethodName)
VALUES
('Alert Email'),
('Cris'),
('Email'),
('In Person'),
('Microsoft Teams'),
('Phone'),
('Ptl'),
('Not Specified');

/****INSERT DATA INTO MR SAFETY CATEGORY TABLE****/

INSERT INTO MRSafetyCategory (MRSafetyName, MRSafetyDescription)
VALUES
('MR Conditional','Deemed safe in the MR environment under defined conditions.'),
('MR Safe','No known hazards resulting from exposure to any MR environment. '),
('MR Unsafe','Poses unacceptable risks to the patient, medical staff or other persons within the MR environment. '),
('MHRA Off-label','Deemed safe to scan outside of defined conditions or if there are no conditions.'),
('Risk Assessment','Risk assessment required before scanning as there are no defined conditions.');

UPDATE MRSafetyCategory
SET MRSafetyName = UPPER(MRSafetyName)

/****INSERT DATA INTO JOB STATUS TABLE****/

INSERT INTO JobStatus (StatusName,StatusDescription)
VALUES
('Waiting','Job logged but not yet started. '),
('Planned','Job logged but not started. MRI planned for future date.'),
('Waiting Others','Job in progress. Physics waiting on information from others to proceed.'),
('Physics Done','MRI Physics safety checks complete.'),
('Complete','Job closed. Ready to book or cancel MRI.'),
('Not Requested','Job closed. MRI not requested by referrer.'),
('Rejected','Job closed. MRI cancelled.');

/****INSERT DATA INTO STAFF TABLE****/

INSERT INTO Staff(FirstName, LastName, Initials)
VALUES
('Jamie','Roberts','JR'),
('Abdelmalek','Benattayallah','AB'),
('Lucy','Wallis','LW'),
('Mike','Mayo','MM'),
('Peter','Wright','PJW'),
('Sarah','Lamerton','SL'),
('Travis','Banks','TB'),
('Nat','Glover','NG'),
('Vicki','La''Roche','VL');

INSERT INTO Staff(FirstName, LastName, Initials)
VALUES
('Not','Assigned','XX')

INSERT INTO Staff(FirstName, LastName, Initials)
VALUES
('Egle','Rackaityte','ER')

/****INSERT DATA INTO JOB TABLE****/

INSERT INTO Job (JobCode, DateJobLogged, PatientId, PatientTypeId, UrgencyId, SiteId, DateMRIRequested, DateMRIPlanned, DateJobCompleted, ImplantId, MRSafetyId)
SELECT 
	Job_Temp.JobId,
	Job_Temp.DateJobLogged,
	Patient.PatientId,
	PatientType.PatientTypeId,
	Urgency.UrgencyId,
	Site.SiteId,
	Job_Temp.MRIRequestDate,
	Job_Temp.DateMRIPlanned,
	Job_Temp.DateJobCompleted,
	ImplantCategory.ImplantId,
	MRSafetyCategory.MRSafetyId
FROM Job_Temp
LEFT JOIN Patient ON (
	Job_Temp.HospitalNumber = Patient.HospitalNumber AND Job_Temp.HospitalNumber IS NOT NULL) 
	OR
	(Job_Temp.HospitalNumber IS NULL AND Job_Temp.NHSNumber = Patient.NHSNumber)
LEFT JOIN PatientType ON Job_Temp.PatientType = PatientType.PatientTypeCode
LEFT JOIN Urgency ON Job_Temp.Urgency = Urgency.UrgencyCode
LEFT JOIN Site ON Job_Temp.Site = Site.SiteShortName
LEFT JOIN ImplantCategory ON Job_Temp.ImplantCategory = ImplantCategory.ImplantName
LEFT JOIN MRSafetyCategory ON Job_Temp.MRSafetyName = MRSafetyCategory.MRSafetyName
;

/****INSERT DATA INTO QUERY TABLE****/

INSERT INTO Query (DateQueryReceived, QueryText, JobId, ContactMethodId)
SELECT
	Job_Temp.DateQueryReceived,
	Job_Temp.QueryFreeText,
	Job.JobId,
	ContactMethod.ContactMethodId
FROM Job_Temp
LEFT JOIN Job ON Job_Temp.JobId = Job.JobCode
LEFT JOIN ContactMethod ON Job_Temp.ContactMethod = ContactMethod.ContactMethodName
;

/****INSERT DATA INTO XRAY CHECK TABLE****/

INSERT INTO XRayCheck (JobId, StaffId)
SELECT 
    Job.JobId,            -- Match JobId from the Job table
    Staff.StaffId          -- Map staff initials to StaffId
FROM 
    Job_Temp
INNER JOIN Job ON Job_Temp.JobId = Job.JobCode
CROSS APPLY 
    STRING_SPLIT(Job_Temp.XRayCheckStaff, '/') XRStaff  -- Split staff initials into rows
INNER JOIN 
    Staff ON XRStaff.value = Staff.Initials; -- Match initials to StaffId

INSERT INTO JobStaff (JobId, StaffId)
SELECT 
    Job.JobId,            -- Match JobId from the Job table
    Staff.StaffId          -- Map staff initials to StaffId
FROM 
    Job_Temp
INNER JOIN Job ON Job_Temp.JobId = Job.JobCode
CROSS APPLY 
    STRING_SPLIT(Job_Temp.StaffAssigned, '/') JobStaff  -- Split staff initials into rows
INNER JOIN 
    Staff ON JobStaff.value = Staff.Initials; -- Match initials to StaffId

/****INSERT DATA INTO STATUS LOG TABLE****/

INSERT INTO StatusLog (JobId,StatusId,ChangedDate)
SELECT
	Job.JobId,
	JobStatus.StatusId,
	Status_Temp.DateStatusChanged
FROM Status_Temp
INNER JOIN Job ON Status_Temp.JobId = Job.JobCode
INNER JOIN JobStatus ON Status_Temp.Status = JobStatus.StatusName
ORDER BY 
    Job.JobId ASC, 
    Status_Temp.DateStatusChanged ASC
;

/****INSERT DATA INTO ACTION LOG TABLE****/

INSERT INTO ActionLog(JobId, PerformedBy, ActionDescription, PerformedDate)
SELECT
	Job.JobId,
	Staff.StaffId,
	Actions_Temp.ActionDescription,
	Actions_Temp.ActionDate
FROM Actions_Temp
LEFT JOIN Job ON Actions_Temp.JobId = Job.JobCode
LEFT JOIN Staff ON Actions_Temp.StaffInitials = Staff.Initials

/****INSERT DATA INTO CRIS COMMENT TABLE****/

ALTER TABLE CRISComment
ADD WrittenBy INT FOREIGN KEY REFERENCES Staff(StaffId)

ALTER TABLE CRISComment
ADD CONSTRAINT FK_CRIS_Staff FOREIGN KEY (WrittenBy) REFERENCES Staff(StaffId)

INSERT INTO CRISComment(JobId, Comment,WrittenBy)
SELECT
	Job.JobId,
	CRIS_Temp.CRISComment,
	Staff.StaffId
FROM CRIS_Temp
LEFT JOIN Job ON CRIS_Temp.JobId = Job.JobCode
LEFT JOIN Staff ON CRIS_Temp.CRISStaff = Staff.Initials
TRUNCATE TABLE CRISComment

