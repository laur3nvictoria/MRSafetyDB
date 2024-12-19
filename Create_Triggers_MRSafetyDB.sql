-- Trigger for ActionLog
CREATE TRIGGER trg_ActionLog_Update
ON ActionLog
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ActionLog
    SET ModifiedDate = GETDATE()
    WHERE ActionId IN (SELECT DISTINCT ActionId FROM Inserted);
END;
GO
-- Trigger for ContactMethod
CREATE TRIGGER trg_ContactMethod_Update
ON ContactMethod
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ContactMethod
    SET ModifiedDate = GETDATE()
    WHERE ContactMethodId IN (SELECT DISTINCT ContactMethodId FROM Inserted);
END;
GO
-- Trigger for CRISComment
CREATE TRIGGER trg_CRISComment_Update
ON CRISComment
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE CRISComment
    SET ModifiedDate = GETDATE()
    WHERE CommentId IN (SELECT DISTINCT CommentId FROM Inserted);
END;
GO
-- Trigger for ImplantCategory
CREATE TRIGGER trg_ImplantCategory_Update
ON ImplantCategory
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ImplantCategory
    SET ModifiedDate = GETDATE()
    WHERE ImplantId IN (SELECT DISTINCT ImplantId FROM Inserted);
END;
GO
-- Trigger for Job
CREATE TRIGGER trg_Job_Update
ON Job
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Job
    SET ModifiedDate = GETDATE()
    WHERE JobId IN (SELECT DISTINCT JobId FROM Inserted);
END;
GO
-- Trigger for JobStaff
CREATE TRIGGER trg_JobStaff_Update
ON JobStaff
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE JobStaff
    SET ModifiedDate = GETDATE()
    WHERE JobStaffId IN (SELECT DISTINCT JobStaffId FROM Inserted);
END;
GO
-- Trigger for JobStatus
CREATE TRIGGER trg_JobStatus_Update
ON JobStatus
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE JobStatus
    SET ModifiedDate = GETDATE()
    WHERE StatusId IN (SELECT DISTINCT StatusId FROM Inserted);
END;
GO
-- Trigger for MRSafetyCategory
CREATE TRIGGER trg_MRSafetyCategory_Update
ON MRSafetyCategory
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE MRSafetyCategory
    SET ModifiedDate = GETDATE()
    WHERE MRSafetyId IN (SELECT DISTINCT MRSafetyId FROM Inserted);
END;
GO
-- Trigger for Patient
CREATE TRIGGER trg_Patient_Update
ON Patient
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Patient
    SET ModifiedDate = GETDATE()
    WHERE PatientId IN (SELECT DISTINCT PatientId FROM Inserted);
END;
GO
-- Trigger for PatientType
CREATE TRIGGER trg_PatientType_Update
ON PatientType
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE PatientType
    SET ModifiedDate = GETDATE()
    WHERE PatientTypeId IN (SELECT DISTINCT PatientTypeId FROM Inserted);
END;
GO
-- Trigger for Query
CREATE TRIGGER trg_Query_Update
ON Query
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Query
    SET ModifiedDate = GETDATE()
    WHERE QueryId IN (SELECT DISTINCT QueryId FROM Inserted);
END;
GO
-- Trigger for Site
CREATE TRIGGER trg_Site_Update
ON Site
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Site
    SET ModifiedDate = GETDATE()
    WHERE SiteId IN (SELECT DISTINCT SiteId FROM Inserted);
END;
GO
-- Trigger for Staff
CREATE TRIGGER trg_Staff_Update
ON Staff
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Staff
    SET ModifiedDate = GETDATE()
    WHERE StaffId IN (SELECT DISTINCT StaffId FROM Inserted);
END;
GO
-- Trigger for StatusLog
CREATE TRIGGER trg_StatusLog_Update
ON StatusLog
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE StatusLog
    SET ModifiedDate = GETDATE()
    WHERE JobStatusId IN (SELECT DISTINCT JobStatusId FROM Inserted);
END;
GO
-- Trigger for Urgency
CREATE TRIGGER trg_Urgency_Update
ON Urgency
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE Urgency
    SET ModifiedDate = GETDATE()
    WHERE UrgencyId IN (SELECT DISTINCT UrgencyId FROM Inserted);
END;
GO
-- Trigger for XRayCheck
CREATE TRIGGER trg_XRayCheck_Update
ON XRayCheck
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE XRayCheck
    SET ModifiedDate = GETDATE()
    WHERE CheckId IN (SELECT DISTINCT CheckId FROM Inserted);
END;
GO

