from sqlalchemy import create_engine

def connect_to_db(server_name, database_name):
    try:
        connection_string = f"mssql+pyodbc://{server_name}/{database_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        engine = create_engine(connection_string)
        print(f"Connection to database {database_name} successful")
        return engine
    except Exception as e:
        print(f"Error connecting to database {database_name} on server {server_name}: {e}")
