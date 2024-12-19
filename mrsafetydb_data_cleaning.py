#%% 
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

#Connect to database on server
def connect_to_db(server_name, database_name):
    try:
        connection_string = f"mssql+pyodbc://{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        engine = create_engine(connection_string)
        print(f"Connection to database {database_name} successful")
        return engine
    except Exception as e:
        print(f"Error connecting to database {database_name} on server {server_name}: {e}")

server_name ="[REDACTED]"
database_name = "CMI_MRSafetyDB"
engine = connect_to_db(server_name, database_name)

#Extract: Read the CSV file
def extract_data(file_path):
    try:
        df=pd.read_csv(file_path)
        print(f"Data extracted successfully from {file_path}.")
        return df
    except Exception as e:
        print(f"Error extracting data from CSV file: {e}")
        return None

file_path = r"G:\CMI\STP Clinical Scientist\SciCom\Lauren Willis\Projects\MRI Safety DB\MRSafetyWorklist-22.11.2024.csv"
df = extract_data(file_path)

# Display first 5 rows
print(df.head())

#%% RENAME COLUMNS

# Change column names to PascalCase for consistency
df.columns = [col.strip().title().replace(" ","") for col in df.columns] #strip() removes leading/trailing spaces, title() capitalises first letter & rest lowercase
print(df.columns)

# Drop unwanted columns from df
unwanted_columns = ['PtId(Rk9,Nhs)','PtlTargetDate','PhysicsTargetDate','PtlOverDue','OverDue/DaysLeft','OverDue/WeeksLeft','Complex?','Comment'] 

def drop_unwanted_columns(df,unwanted_columns):
    for col in unwanted_columns:
        if col in df.columns:
            df.drop(col, axis=1,inplace=True)       
            print(f"Column {col} dropped from DateFrame.")
        else:
            print(f"Column {col} not found in DataFrame.")
    return df

df = drop_unwanted_columns(df,unwanted_columns)
# Print output
print(df.columns)   

# Define new column names in dictionary
new_column_names = {                                # Most column names in spreadsheet do not match up with columns in database. To simplify loading, will rename those columns now
    "Date Job Logged":"DateJobLogged",              
    "PtId":"HospitalNumber",
    "Jobid":"JobId",
    "DateQueryRaised":"DateQueryReceived",
    "PtId(NhsNumber)":"NHSNumber",
    "PtInitials":"Initials",
    "RequestDate":"MRIRequestDate",
    "QueryType":"ContactMethod",
    "Planned/AppDate":"DateMRIPlanned",
    "Query":"QueryFreeText",
    "CrisComment":"CRISComment",
    "Intitals(WorkedOn)":"StaffAssigned",
    "MrCond":"MRSafetyName",
    "XrayCheck":"XRayCheckStaff",
    "Status":"MidStatus",
    "RespondedOn":"DateMidStatusChanged",
    "Status.1":"Status",
    "ReadyToBookOrCancelled":"DateJobCompleted"
}
df = df.rename(columns=new_column_names)
# Print output
print(df.columns)
#%% DATA QUALITY CHECKS

# Find number of records in df
num_records = len(df.index)
print(f"Number of records extracted from CSV: {num_records}")

# Find number of null values in each column
print(df.isnull().sum())

# Find data type of each column
print(df.dtypes)

# Check for duplicate and blank jobs 
def check_jobid(df):

    # Drop duplicate jobs with same JobId & same patient
    duplicate_jobs = df.duplicated(subset=["JobId","HospitalNumber","NHSNumber"], keep=False)
    if not duplicate_jobs.empty:
        # Drop duplicate jobs
        df = df.drop_duplicates(subset=["JobId","HospitalNumber","NHSNumber"],keep="first")
        # Calculate number of duplicate drops dropped
        num_duplicate_jobs_dropped = num_records - len(df.index)
        # Print output
        print(f"{num_duplicate_jobs_dropped} duplicate jobs were dropped from the DataFrame.")
    else:
        print("No duplicate jobs found.")

    # Drop blank jobs with no status
    blank_jobs = df[df["Status"].isna()]
    if not blank_jobs.empty:
        print(f"{len(blank_jobs)} blank jobs were dropped from the DataFrame.")
        df = df.dropna(subset=['Status'])
    else:
        print("No blank jobs found.")
    
    print(f"Number of records after blank/duplicate jobs dropped: {len(df.index)} of which {df['JobId'].nunique()} are unique. ")
  
    return df

df = check_jobid(df)

#%% DATA CLEANING
# This section defines a function to clean the data in each column of the df

# Hospital Number
def clean_hospital_number(df):
    # To be valid, hospital number must be in format of 1 letter & 6 numbers or 6 numbers only
    valid_pattern = r"^[A-Za-z]{1}\d{6}$|^\d{6}$"   
    # Removes any non-digit or non-letter characters from string including spaces
    df["HospitalNumber"] = df["HospitalNumber"].str.replace(r"[^A-Za-z0-9]","",regex=True) 
    # Replaces empty strings as a result of previous step with NaN so they are ignored     
    df["HospitalNumber"] = df["HospitalNumber"].replace("",np.nan)  
    #Capitalise all letters in string  
    df["HospitalNumber"] = df["HospitalNumber"].str.upper()  
    #RK9 is the hospital ODS code. If added to string, it is too long. Drop the "RK9" prefix from any row.   
    df["HospitalNumber"] = df["HospitalNumber"].str.removeprefix("RK9")     
    
    # If NHS number is entered as hospital number
    for index, row in df.iterrows():
        if isinstance(row["HospitalNumber"], str) and row["HospitalNumber"].isdigit() and len(row["HospitalNumber"]) == 10:
            # Assign the 10-digit hospital number to NHSNumber
            df.at[index, "NHSNumber"] = row["HospitalNumber"]
            # Replace the hospital number with NaN
            df.at[index, "HospitalNumber"] = np.nan

    #Verify if format of hospital number is valid. Creates new column in df. 
    df["HospitalNumber_valid"]=df["HospitalNumber"].str.match(valid_pattern,na=True)   
    # Filters df to find invalid hospital numbers
    invalid_hospital_numbers = df[df["HospitalNumber_valid"] == False]
    # Prints invalid hospital numbers
    print(invalid_hospital_numbers)
    return df

df = clean_hospital_number(df)

# NHS Number
def clean_nhs_number(df):
    # Valid format of NHS number is 10 digits, usually presented as 123 456 7890
    # Remove any non-digit characters, including spaces
    df["NHSNumber"] = df["NHSNumber"].str.replace(r"[^\d]", "", regex=True) 
    # Replace empty strings with NaN
    df["NHSNumber"] = df["NHSNumber"].replace("", np.nan)

    # Filter rows where NHS Number is not null and its length is not equal to 10. 
    invalid_nhs_numbers = df.loc[
    df["NHSNumber"].notna() & (df["NHSNumber"].str.len() != 10), "NHSNumber"
    ]
    # Print list of invalid NHS numbers
    print(invalid_nhs_numbers.tolist())
    return df

df = clean_nhs_number(df)

# Patient initials
def clean_patient_initials(df):
    # Remove any non-letter characters, including spaces
    df["Initials"] = df["Initials"].str.replace(r"[^A-Za-z]","",regex=True)
    # Replace empty strings with NaN
    df["Initials"] = df["Initials"].replace("", np.nan)
    # Make all letters uppercase for consistency
    df["Initials"] = df["Initials"].str.upper()
    # Truncate strings longer than 5 characters
    df["Initials"] = df["Initials"].str.slice(0,5)
    return df

df = clean_patient_initials(df)

# Prints rows that have no patient identifier 
print(df[df["HospitalNumber"].isnull() & df["NHSNumber"].isnull()])

# Patient identifiers
def clean_patient(df):
    # Create a mapping between 'HospitalNumber' and 'NHSNumber' by dropping rows where either is NULL
    # Forward mapping: create dictionary of HospitalNumber -> NHSNumber
    patient_mapping = df.dropna(subset=['HospitalNumber','NHSNumber']).set_index('HospitalNumber')['NHSNumber'].to_dict()
    # Reverse mapping: create dictionary of NHSNumber -> HospitalNumber
    patient_mapping_reverse = df.dropna(subset=['HospitalNumber','NHSNumber']).set_index('NHSNumber')['HospitalNumber'].to_dict()
    # Combine forward and reverse mappings into a single dictionary
    patient_mapping.update(patient_mapping_reverse)

    # Initialise counter to track the number of updates made
    updated_patient = 0

    # Iterate through each row in the df to fill missing patient data
    for idx, row in df.iterrows():
        # If hospital number is missing but NHS number exists, look up the mapping and update it
        if pd.isna(row['HospitalNumber']) and pd.notna(row['NHSNumber']):
            nhs_number = row['NHSNumber']
            if nhs_number in patient_mapping:
                df.at[idx, 'HospitalNumber'] = patient_mapping[nhs_number]
                updated_patient +=1
        # If NHS number is missing but hospital number exists, look up the mapping and update it
        elif pd.isna(row['NHSNumber']) and pd.notna(row['HospitalNumber']):
            hospital_number = row['HospitalNumber']
            if hospital_number in patient_mapping:
                df.at[idx, 'NHSNumber'] = patient_mapping[hospital_number]
                updated_patient += 1
    # Print the total number of updates made to the df
    print(f"Number of patients updated: {updated_patient}")
    return df       # Return cleaned df

df = clean_patient(df)

# Date columns
def convert_dates(df,date_columns):
    # Convert each column in 'date_columns' to datetime format with a specified date format
    # Use "coerce" to handle invalid formats by converting them to NaT (Not a Time)
    for col in date_columns:
        df[col] = pd.to_datetime(df[col],format="%d/%m/%Y",errors="coerce")
    return df # Return the DataFrame with converted date columns.

    # Define a nested function to check the sequence of dates in each row
    '''
    def check_date_sequence(row):
        for i in range(1,len(date_columns)):
            current_date = row[date_columns[i]]
            next_date = row[date_columns[i+1]]
            if pd.notnull(next_date) and pd.notnull(current_date):
                if next_date < current_date:
                    return False
            return True
    df[date_sequence_valid] = df.apply(check_date_sequence, axis=1)
    invalid_rows = df[df['date_sequence_valid'] == False]
    print(f"Invalid rows:{invalid_rows}")
    return df
    '''

# List all date columns in df to be converted to datetime format
date_columns = ["DateJobLogged","DateQueryReceived","MRIRequestDate","DateMRIPlanned","DateMidStatusChanged","PhysicsDone","DateJobCompleted"]
df = convert_dates(df,date_columns)

# Fills in NULL values in DateJobLogged with corresponding value in DateQueryReceived
df['DateJobLogged'] = df['DateJobLogged'].fillna(df['DateQueryReceived'])

# Urgency
def clean_urgency(df):
    # Remove any non-digit characters, including spaces
    df["Urgency"] = df["Urgency"].str.replace(r"[^\d]", "", regex=True) 
    # Replace empty strings with NaN
    df["Urgency"] = df["Urgency"].replace("", np.nan)
    # Replace NaN values with 0 -- for int conversion
    df["Urgency"] = df["Urgency"].fillna(0)
    # Convert urgency data type to int
    df["Urgency"] = df["Urgency"].astype(int)
    return df       # Return cleaned df

df = clean_urgency(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["Urgency"].unique())

# Site
def clean_site(df):
    # Strip white space for consistency
    df["Site"] = df["Site"].str.strip()
    # Make all letters uppercase for consistency
    df["Site"] = df["Site"].str.upper()
    # Replace NaN values with 'not specified'
    df["Site"] = df["Site"].fillna("NOT SPECIFIED")
    # Change all variations of Truro/RCHT to TRURO
    df["Site"] = df["Site"].apply(
        lambda x: "TRURO" if "TRURO" in x else x
    )
    # Change all variations of Inhealth to INHEALTH
    df["Site"] = df["Site"].apply(
        lambda x: "INHEALTH" if "INHEALTH" in x else x
    )
    return df       # Return cleaned df

df = clean_site(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["Site"].unique())

# Contact method
def clean_contact_method(df):
    # Strip white space for consistency
    df["ContactMethod"] = df["ContactMethod"].str.strip()
    # Make all letters lowercase for consistency
    df["ContactMethod"] = df["ContactMethod"].str.lower()
    # Replace NaN values with 'not specified'
    df["ContactMethod"] = df["ContactMethod"].fillna("not specified")
    # Change all variations of team/teams/MS teams to 'microsoft teams'
    df["ContactMethod"] = df["ContactMethod"].apply(
        lambda x: "microsoft teams" if "team" in x else x
    )
    # Change all rows containing 'cris' to 'cris'
    df["ContactMethod"] = df["ContactMethod"].apply(
        lambda x: "cris" if "cris" in x else x
    )
    # Make the first letter of each word in string capitalised
    df["ContactMethod"] = df["ContactMethod"].str.title()
    return df       # Return cleaned df

df = clean_contact_method(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["ContactMethod"].unique())

# Patient type
def clean_patient_type(df):
    # Strip white space for consistency
    df["PatientType"] = df["PatientType"].str.strip()
    # Replace NaN values with 'not specified'
    df["PatientType"] = df["PatientType"].fillna("ns")
    # Capitalise for consistency
    df["PatientType"] = df["PatientType"].str.upper()
    return df       # Return cleaned df

df = clean_patient_type(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["PatientType"].unique())

# Query
def clean_query_free_text(df):
    # Strip white space
    df["QueryFreeText"] = df["QueryFreeText"].str.strip()
    # Find maximum length of query string
    max_query_length = df["QueryFreeText"].str.len().max()
    # Print maximum length of query string
    print(f"Maximum string length is: {max_query_length}")
    return df       # Return cleaned df

df = clean_query_free_text(df)

# Implant category
def clean_implant_category(df):
    # Strip white space for consistency
    df["ImplantCategory"] = df["ImplantCategory"].str.strip()
    # Replace NaN values with 'not stated'
    df["ImplantCategory"] = df["ImplantCategory"].fillna("Not Stated")
    # Capitalise for consistency
    df["ImplantCategory"] = df["ImplantCategory"].str.upper()
    return df       # Return cleaned df

df = clean_implant_category(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["ImplantCategory"].unique())

# Staff initials
def clean_staff(df,staff_columns):
    for col in staff_columns:
        # Strip white space for consistency
        df[col] = df[col].str.strip()
        # Capitalise for consistency
        df[col] = df[col].str.upper()
        # Make sure delimiter between staff assigned is '/', replace other delimiters like , & \
        df[col] = df[col].str.replace(r"[,& \\]", "/", regex=True)
    return df       # Return cleaned df

# Define columns that contain staff initials
staff_columns = ["StaffAssigned","XRayCheckStaff"]

df = clean_staff(df,staff_columns)

# Verify that cleaning is sufficient by printing unique values in columns
print(df["StaffAssigned"].unique())
print(df["XRayCheckStaff"].unique())

# MR safety category
def clean_mr_safety_category(df):
    # Strip white space for consistency
    df["MRSafetyName"] = df["MRSafetyName"].str.strip()
    # Capitalise for consistency
    df["MRSafetyName"] = df["MRSafetyName"].str.upper()
    # Replace abbreviations in data with safety category name
    df["MRSafetyName"] = df["MRSafetyName"].apply(
        lambda x: 'MR CONDITIONAL' if x == 'MR COND' else x
    )
    df["MRSafetyName"] = df["MRSafetyName"].apply(
        lambda x: 'RISK ASSESSMENT' if x == 'RA' else x
    )
    return df       # Return cleaned df

df = clean_mr_safety_category(df)
# Verify that cleaning is sufficient by printing unique values in column
print(df["MRSafetyName"].unique())

#%% LOAD STATUS

# Create new dataframe which contains only relevant columns to status
df_status = df[['JobId','MidStatus','DateMidStatusChanged','PhysicsDone','Status','DateJobCompleted','DateQueryReceived']]
# Create empty array
job_status_data = []

# Iterate through each row of the df_status DataFrame
for idx, row in df_status.iterrows():
    # Extract relevant data from the current row
    job_id = row['JobId']  
    mid_status = row['MidStatus']
    status = row['Status']
    date_status = row['DateJobCompleted']
    date_physics_done = row['PhysicsDone']
    date_mid_status = row['DateMidStatusChanged']
    date_query_received = row['DateQueryReceived']

    # If 'PhysicsDone' date is present, add a "Physics Done" status entry
    if pd.notna(date_physics_done):
        job_status_data.append({'JobId':job_id, 'Status':"Physics Done",'DateStatusChanged':date_physics_done})

    # Handle the "Planned" and "Waiting" statuses, using 'DateQueryReceived' as the status change date
    if status == "Planned":
        job_status_data.append({'JobId': job_id, 'Status': "Planned", 'DateStatusChanged': date_query_received})
    if status == "Waiting":
        job_status_data.append({'JobId': job_id, 'Status': "Waiting", 'DateStatusChanged': date_query_received})
    # For other statuses, use 'DateJobCompleted' as the status change date
    else:
        job_status_data.append({'JobId':job_id, 'Status':status,'DateStatusChanged':date_status})
    # Add an entry for the "Waiting Others" status if applicable
    if mid_status == "Waiting Others":
        job_status_data.append({'JobId':job_id, 'Status':mid_status,'DateStatusChanged':date_mid_status})

# Create a new DataFrame from the collected data
df_status_new = pd.DataFrame(job_status_data)

# Print the number of records in the new DataFrame
print(len(df_status_new))
# Remove duplicate entries based on the combination of 'JobId' and 'Status'
df_status_new = df_status_new.drop_duplicates(subset=['JobId', 'Status'])
# Print the number of records after removing duplicates
print(len(df_status_new))

# Save the cleaned DataFrame to a CSV file named 'jobstatushistorytable.csv' for further inspection
df_status_new.to_csv('jobstatushistorytable.csv', index=False)

# Load the data into a temporary staging table in the database
df_status_new.to_sql('Status_Temp', con=engine, if_exists='append', index=False)

# %% LOAD ACTION LOG
import re
import pandas as pd

# Create new DataFrame with columns relevant to actions
df_comments = df[['JobId', 'NextAction', 'CRISComment', 'StaffAssigned']]

# Prepend 'UPDATE' to ensure uniformity in processing
df_comments['NextAction'] = df_comments['NextAction'].apply(
    lambda x: f"UPDATE: {x}" if pd.notna(x) and not re.search(r'\bupdate\b', x, re.IGNORECASE) else x
)

# Create empty array
action_table_data = []

# Iterate through each row of the df_comments DataFrame
for idx, row in df_comments.iterrows():
    # Extract the Job ID and the NextAction column value
    job_id = row['JobId']
    actions = row['NextAction']

    # Skip rows where NextAction is empty or NaN
    if pd.isna(actions):
        continue

    # Use regex to split actions into separate updates based on the keyword "UPDATE"
    action_list = re.split(r'(?=UPDATE\b)', actions)

    # Process each individual action in the split list
    for action in action_list:
        action = action.strip()     # Remove extra whitespace
        if not action:  # Skip empty splits
            continue

        # Match the pattern "UPDATE {StaffInitials} {Date}: {Description}" using regex
        action_match = re.match(
            r'UPDATE\s*([A-Z]{2,3})?\s*(\d{2}/\d{2}(?:/\d{4})?)?\s*:\s*(.*)', action
        )

        if action_match:
            # Extract staff initials, action date, and description from the matched groups
            staff_initials = action_match.group(1)
            action_date = action_match.group(2)
            action_description = action_match.group(3).strip()

            # Convert dates in dd/mm format to include the year 2024
            if action_date:
                if re.match(r'^\d{2}/\d{2}$', action_date):  # If year is missing
                    action_date = f"{action_date}/2024"
                # Convert the action date to a pandas datetime object
                action_date = pd.to_datetime(action_date, format='%d/%m/%Y', errors='coerce')
            
            #Append the parsed data to the action_table_data list
            action_table_data.append({
                'JobId': job_id,
                'StaffInitials': staff_initials,
                'ActionDate': action_date,
                'ActionDescription': action_description
            })
        else:
            # Handle improperly formatted "UPDATE" entries by storing them with limited details
            action_table_data.append({
                'JobId': job_id,
                'StaffInitials': None,
                'ActionDate': None,
                'ActionDescription': action
            })

# Convert the collected action log data into a DataFrame
df_actions_new = pd.DataFrame(action_table_data)
# Save the action log data to a CSV file for further inspection
df_actions_new.to_csv('actionlog.csv', index=False)

# Load the data into a temporary staging table in the database
df_actions_new.to_sql('Actions_Temp', con=engine, if_exists='append', index=False)

# %% LOAD PATIENT

def load_patient(df):
    # Create new patient dataframe with relevant columns for patient table
    df_patient = df[["HospitalNumber", "NHSNumber", "Initials"]]

    # Remove any completely blank rows
    df_patient = df_patient.dropna(how="all",subset=["HospitalNumber","NHSNumber"])
    print(len(df_patient))      # Print number of records in df

    # Make a copy of the DataFrame to avoid modifying the original
    df_patient = df_patient.copy()

    # Add a new column, 'PatientCode', which uniquely identifies a patient.
    # - Use 'NHSNumber' if 'HospitalNumber' is missing.
    # - Otherwise, use 'HospitalNumber' prefixed with "RK9".
    df_patient.loc[:, 'PatientCode'] = df_patient.apply(
    lambda row: row['NHSNumber'] if pd.notnull(row['NHSNumber']) and pd.isnull(row['HospitalNumber'])
    else (f"RK9{row['HospitalNumber']}" if pd.notnull(row['HospitalNumber']) else None),
    axis=1
    )
    # Remove duplicate patients based on 'PatientCode', keeping only the first occurrence
    df_patient = df_patient.drop_duplicates(subset=["PatientCode"],keep = "first")
    print(len(df_patient))      # Print number of records in df

    # Save the cleaned patient table data to a CSV file for further inspection
    df_patient.to_csv('patienttable.csv', index=False)
    # Drop the 'PatientCode' column from the DataFrame before importing
    df_patient = df_patient.drop(columns=['PatientCode'], errors='ignore')

    # Load the remaining data into the 'Patient' table in the database
    df_patient.to_sql('Patient', con=engine, if_exists='append', index=False)

    return df_patient

df_patient = load_patient(df)

# %% LOAD JOB

# Create a new DataFrame with only relevant columns for job table
df_job = df.drop(columns=['Initials','NextAction','CRISComment','HospitalNumber_valid'])

# Save the job data to a CSV file for further inspection
df_job.to_csv('jobdata.csv',index=False)

# Load the job data into a temporary staging table in the database
df_job.to_sql('Job_Temp', con=engine, if_exists='append', index=False)
# %% LOAD CRIS COMMENT

# Create a new DataFrame with only relevant columns
df_cris = df[['JobId','CRISComment', 'StaffAssigned','DateJobCompleted']]

# Create empty array
cris_table_data = []

# Iterate through each row of the df_cris DataFrame
for idx, row in df_cris.iterrows():
    # Extract relevant fields from df
    job_id = row['JobId']
    cris_comment = row['CRISComment']
    staff_assigned = row['StaffAssigned']
    date_job_completed = row['DateJobCompleted']
    
    # Skip rows where CRISComment is NaN or empty
    if pd.isna(cris_comment):
        continue

    # Strip leading/trailing spaces and new lines from CRISComment
    cris_comment = cris_comment.strip()

    # Skip if CRISComment is still empty after stripping
    if cris_comment == "":
        continue

    # Determine cris_staff based on CRISComment content
    if pd.notna(cris_comment) and "JR (MRSE)" in cris_comment:
        cris_staff = "JR"
    else:
        cris_staff = staff_assigned  # Use StaffAssigned if condition not met
    
    # Use DateJobCompleted for date_cris
    date_cris = date_job_completed

    # Append to CRIS log data
    cris_table_data.append({
        'JobId': job_id,
        'CRISComment': cris_comment,
        'CRISStaff': cris_staff,
        'DateCRIS': date_cris
    })

# Convert to a DataFrame
df_cris_log = pd.DataFrame(cris_table_data)

# Save the CRIS comment data to a CSV file for further inspection
df_cris_log.to_csv('cris_log.csv', index=False)

# Load the CRIS comment data into a temporary staging table in database
df_cris_log.to_sql('CRIS_Temp', con=engine, if_exists='append', index=False)
