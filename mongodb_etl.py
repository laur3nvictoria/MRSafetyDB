#%% CONNECT TO DATABASES
from sqlalchemy import create_engine
from db_connection import connect_to_db
import pandas as pd
from pymongo import MongoClient

#Connect to SQL server database
server_name ="[REDACTED]"
database_name = "CMI_MRSafetyDB"
engine = connect_to_db(server_name, database_name)

#Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # MongoDB connection string
db = client["MRSafetyDB"]  # MongoDB database name

# %% PATIENT COLLECTION

patient_collection = db["patient"]

# Select columns from patient table in SQL server DB
patient_df = pd.read_sql("SELECT PatientId,HospitalNumber,NHSNumber,Initials,PatientCode FROM Patient",engine)

# Convert data frame to dictionary for pymongo
patient_data = patient_df.to_dict(orient="records")

# Initialise counter for number of documents upserted
patient_document_count = 0

for record in patient_data:
    # Match records in MongoDB and SQL server DB based on PatientId (primary key)
    query = {"patientKey": record["PatientId"]}

    # Fields to be inserted or updated 
    update_data = {
        "$set": {
            "patientKey": record["PatientId"],         
            "hospitalNumber": record.get("HospitalNumber"),
            "nhsNumber": record.get("NHSNumber"),
            "patientCode": record.get("PatientCode"),
            "initials": record.get("Initials")         
        }
    }

    try:
        # Attempt to update a single document in the collection matching the query.
        # If no document matches and upsert=True, a new document will be created.
        result = patient_collection.update_one(query, update_data, upsert=True)
        patient_document_count+=1
    except Exception as e:
        # Handle any exceptions during the update operation and log the error 
        print(f"Error processing record {record}: {e}")
# Print summary
print(f'{patient_document_count} of {len(patient_data)} records successfully upserted into collection.')
# %% STAFF COLLECTION
staff_collection = db["staff"]

# Select columns from patient table in SQL server DB
staff_df = pd.read_sql("SELECT StaffId, FirstName, LastName, Initials FROM Staff",engine)

# Convert data frame to dictionary for pymongo
staff_data = staff_df.to_dict(orient="records")

# Initialise counter for number of documents upserted
staff_document_count = 0

for record in staff_data:
    # Match records in MongoDB and SQL server DB based on PatientId (PK)
    query = {"staffKey": record["StaffId"]}

    # Fields to be inserted or updated 
    update_data = {
        "$set": {
            "staffKey": record["StaffId"],    
            "staffInitials": record.get("Initials"),        
            "firstName": record.get("FirstName"),
            "lastName": record.get("LastName")
      
        }
    }

    try:
        # Attempt to update a single document in the collection matching the query.
        # If no document matches and upsert=True, a new document will be created.
        result = staff_collection.update_one(query, update_data, upsert=True)
        staff_document_count+=1
    except Exception as e:
        # Handle any exceptions during the update operation and log the error 
        print(f"Error processing record {record}: {e}")
# Print summary
print(f'{staff_document_count} of {len(staff_data)} records successfully upserted into collection.')

# %% JOB COLLECTION

# Reference job collection on MongoDB database
job_collection = db["job"]      

# Initialise counter for number of documents upserted
job_document_count = 0

# SQL query to retrieve job-related data from OLTP database
job_query = """
    SELECT 
        j.JobId,
        j.JobCode,
        q.DateQueryReceived,
        q.QueryText,
        j.DateJobLogged,
        cm.ContactMethodName,
        j.PatientId,
        s.SiteShortName,
        pt.PatientTypeName,
        u.UrgencyCode,
        u.UrgencyType,
        j.DateMRIRequested,
        j.DateMRIPlanned,
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
        cris.Comment AS CRISComment,
        cris.WrittenBy AS CRISCommentWrittenBy
    FROM Job j
    JOIN Urgency u ON j.UrgencyId = u.UrgencyId
    LEFT JOIN Query q ON j.JobId = q.JobId
    LEFT JOIN ContactMethod cm ON q.ContactMethodId = cm.ContactMethodId
    LEFT JOIN ImplantCategory i ON j.ImplantId = i.ImplantId
    LEFT JOIN Site s ON j.SiteId = s.SiteId
    LEFT JOIN MRSafetyCategory mr ON j.MRSafetyId = mr.MRSafetyId
    LEFT JOIN PatientType pt ON j.PatientTypeId = pt.PatientTypeId
    LEFT JOIN CRISComment cris ON j.JobId = cris.JobId
    """

# Extract job data and load into a DataFrame
job_df = pd.read_sql(job_query,engine)
# Replace null values with None
job_df = job_df.where(pd.notnull(job_df), None)

# Convert the DataFrame to a list of dictionaries for MongoDB insertion
job_data = job_df.to_dict(orient="records")

# Extract data from StatusLog table and transform it into a dictionary grouped by JobId
status_query = """
    SELECT
        sl.JobId, 
        jst.StatusName,
        sl.ChangedDate
    FROM StatusLog sl
    LEFT JOIN JobStatus jst ON sl.StatusId = jst.StatusId

    """

status_df = pd.read_sql(status_query,engine)

status_df["ChangedDate"] = status_df["ChangedDate"].where(pd.notnull(status_df["ChangedDate"]), None)
status_data = status_df.groupby("JobId").apply(
    lambda x: [
        {
            "StatusName": row["StatusName"],
            "ChangedDate": row["ChangedDate"].to_pydatetime() if pd.notnull(row["ChangedDate"]) else None
        }
        for _, row in x.iterrows()
    ]
).to_dict()
# Verify transformation by printing first 5 rows
print(status_df.head())

# Extract data from ActionLog table and transform it into a dictionary grouped by JobId
action_query = """
    SELECT
        JobId,
        ActionDescription,
        PerformedBy,
        PerformedDate
    FROM ActionLog
    """

action_df = pd.read_sql(action_query,engine)
action_df = action_df.where(pd.notnull(action_df), None)
action_df["PerformedBy"]= action_df["PerformedBy"].astype("Int64")
action_data = action_df.groupby("JobId").apply(
    lambda x: [
        {
            "comment": row["ActionDescription"],
            "staffMember":row["PerformedBy"],
            "date": row["PerformedDate"].to_pydatetime() if pd.notnull(row["PerformedDate"]) else None
        }
        for _, row in x.iterrows()
    ]
).to_dict()
# Verify transformation by printing first 5 rows
print(action_df.head())

# Extract and transform staff data
job_staff_query = """
    SELECT
        JobId, 
        StaffId
    FROM JobStaff
    """
job_staff_df = pd.read_sql(job_staff_query,engine)
job_staff_df = job_staff_df.where(pd.notnull(job_staff_df),None)
job_staff_data = (
    job_staff_df.groupby("JobId")["StaffId"]
    .apply(list)
    .to_dict()
    )

# Extract and transform xray check data
xray_check_query = """
    SELECT
        JobId, 
        StaffId
    FROM XRayCheck
    """
xray_check_df = pd.read_sql(xray_check_query,engine)
xray_check_df = xray_check_df.where(pd.notnull(xray_check_df),None)
xray_check_data = (
    xray_check_df.groupby("JobId")["StaffId"]
    .apply(list)
    .to_dict()
    )

print(xray_check_df.head())

# Function to get ObjectID for a patient already in MongoDB
def get_patient_object_id(patient_id):
    patient = patient_collection.find_one({"patientKey": patient_id})
    return patient["_id"] if patient else None

# Function to get ObjectID for staff members already in MongoDB
def get_staff_object_ids(ids, field_name, collection):

    staff_object_ids = []
    for id_ in ids:
        doc = collection.find_one({field_name: id_}, {"_id": 1})
        if doc:
            staff_object_ids.append(doc["_id"])
    return staff_object_ids

# Iterate over the job data
for _, job in job_df.iterrows():
    # Match the job by JobId
    query = {"jobKey": job["JobId"]}        # Match job by its JobId
    
    # Construct update_data for MongoDB
    update_data = {
        "$set": {
            "jobKey": job["JobId"],
            "jobCode": job["JobCode"],
            "patientDetails": {
                "patientId": get_patient_object_id(job["PatientId"]) if pd.notnull(job["PatientId"]) else None,
                "patientType": job["PatientTypeName"] if job["PatientTypeName"] else None,
            },
            "scanDetails": {
                **({"dateMRIRequested": job["DateMRIRequested"]} if pd.notnull(job["DateMRIRequested"]) else {}),
                **({"dateMRIScheduled": job["DateMRIPlanned"]} if pd.notnull(job["DateMRIPlanned"]) else {}),
                "urgency": {
                    "urgencyCode": job["UrgencyCode"] if job["UrgencyCode"] else None,
                    "urgencyType": job["UrgencyType"] if job["UrgencyType"] else None,
                },
                "site": job["SiteShortName"] if job["SiteShortName"] else None,
            } ,
            "queryDetails": {
                "dateReceived": job["DateQueryReceived"].to_pydatetime() if pd.notnull(job["DateQueryReceived"]) else [],
                "receivedBy": job.get("ContactMethodName"),
                "message": job.get("QueryText")
            },
            
            "safetyInvestigation": {
                "dateJobLogged": job.get("DateJobLogged"),
                "staffAssigned": get_staff_object_ids(
                    job_staff_data.get(job["JobId"], []),  # List of staff IDs
                    "staffKey",                            # Field to match on
                    staff_collection                       # MongoDB collection
                ),

                "implantCategory": job.get("ImplantName"),
                "mrSafety": job.get("MRSafetyName"),
                **({"xrayCheckedBy" :get_staff_object_ids(xray_check_data.get(job["JobId"],[]),"staffKey",staff_collection)}if xray_check_data.get(job["JobId"]) else {}),
            # ** Optional field
                **({"actionLog": [
                    {
                        **action,  # Include all existing fields in the action
                        "staffMember": (
                            get_staff_object_ids(
                                [int(action["staffMember"])], "staffKey", staff_collection
                            )[0]  # Get the first (and only) ObjectId
                            if pd.notnull(action["staffMember"]) and action["staffMember"] is not None
                            else None  # Set to None if staffMember is null
                        )
                    }
                    for action in action_data.get(job["JobId"], [])
                ]
            } if action_data.get(job["JobId"]) else {}),
            # ** Optional field
                **({"CRIS": {
                    "comment": job.get("CRISComment"),
                    "writtenBy": job.get("CRISCommentWrittenBy")
                }} if job.get("CRISComment") else {})
            },

            "statusLog": status_data.get(job["JobId"], []),

            "kpiPerformance": {
                "ptlTargetDate": job["PTLTargetDate"].to_pydatetime() if pd.notnull(job["PTLTargetDate"]) else None,
                "physicsTargetDate": job["PhysicsTargetDate"].to_pydatetime() if pd.notnull(job["PhysicsTargetDate"]) else None,
            }
        }
    }
    # Perform the upsert operation in MongoDB
    try:
        result = job_collection.update_one(query, update_data, upsert=True)
        job_document_count+=1
    except Exception as e:
        print(f"Error processing JobId {job['JobId']}: {e}")
print(f'{job_document_count} of {len(job_data)} records successfully upserted into collection.')

# %%
