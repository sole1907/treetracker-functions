from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import psycopg2
import pandas as pd
import re
from decimal import Decimal

load_dotenv(override=True)

src_db_host = os.environ.get("SRC_DB_HOST")
src_db_port = os.environ.get("SRC_DB_PORT")
src_db_name = os.environ.get("SRC_DB_NAME")
src_db_user = os.environ.get("SRC_DB_USER")
src_db_password = os.environ.get("SRC_DB_PASSWORD")

dest_db_host = os.environ.get("DEST_DB_HOST")
dest_db_port = os.environ.get("DEST_DB_PORT")
dest_db_name = os.environ.get("DEST_DB_NAME")
dest_db_user = os.environ.get("DEST_DB_USER")
dest_db_password = os.environ.get("DEST_DB_PASSWORD")

# Database connection details for dbone
dbone_config = {
    "host": src_db_host,
    "port": src_db_port,
    "dbname": src_db_name,
    "user": src_db_user,
    "password": src_db_password
}

# Database connection details for dbtwo
dbtwo_config = {
    "host": dest_db_host,
    "port": dest_db_port,
    "dbname": dest_db_name,
    "user": dest_db_user,
    "password": dest_db_password
}

#function to read properties file as a json object
def read_properties_file(file_path):
    properties = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    properties[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"File {file_path} not found.")
    return properties

print("Loading properties...")
# load properties
loaded_properties = read_properties_file("config.properties")

def escape_str(value):
    return value.replace("'", "''")

def generate_insert_queries(df, table_name):
    try:
        # Initialize the query
        query = f"INSERT INTO {table_name} ("

        # Get the column names
        columns = df.columns.tolist()

        # Add column names to the query
        query += ", ".join(columns) + ") VALUES ("

        # Iterate over each row in the dataframe
        for index, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if isinstance(value, (int, float, Decimal)):
                    # If value is an integer or float, add it without quotes
                    values.append(f"NULL" if pd.isnull(value) else str(value))
                else:
                    # Otherwise, add it with quotes
                    values.append(f"NULL" if pd.isnull(value) else (f"'{value}'" if isinstance(value, pd.Timestamp) else f"'{escape_str(value)}'"))

            query += ", ".join(values) + "), ("

        # Remove the trailing ", (" and add a semicolon at the end
        query = query[:-3] + ";"

    except (Exception) as error:
        print(f"Error generating insert query: {error}")

    return query

def generate_update_queries(df, table_name, identifier_column="id"):
    try:
        query_prefix = f"UPDATE {table_name} SET "

        # Get the column names
        columns = df.columns.tolist()
        update_queries = [] 

        # Iterate over each row in the dataframe
        for index, row in df.iterrows():
            identifier_value = row[identifier_column]
            updates = []
            for col in columns:
                if (col == identifier_column):
                    continue
                value = row[col]
                if isinstance(value, (int, float, Decimal)):
                    # If value is an integer or float, add it without quotes
                    value = f"NULL" if pd.isnull(value) else str(value)
                    updates.append(f"{col} = {value}")
                else:
                    # Otherwise, add it with quotes
                    value = f"NULL" if pd.isnull(value) else (f"'{value}'" if isinstance(value, pd.Timestamp) else f"'{escape_str(value)}'")
                    updates.append(f"{col} = {value}")

            query = query_prefix + ", ".join(updates) + " WHERE " + f"{identifier_column} = {identifier_value};"
            update_queries.append(query)

    except (Exception) as error:
        print(f"Error generating update query: {error}")

    return update_queries

def extract_table_name(query):
    # Regular expression pattern to match the table name
    pattern = r"from\s+(\w+)(?:\s*,|\s+where|$)"

    # Search for the pattern in the query
    match = re.search(pattern, query, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None
    
def process_new_records(db_config, df_dbone, df_dbtwo, table_name, columns):
    # Compare the DataFrames to find new records.
    # Merge the two DataFrames with an indicator flag
    merged_df = df_dbone.merge(df_dbtwo, on='id', suffixes=("_df1", "_df2"), how='left', indicator=True)
    
    # Filter out the rows that are only in dataframe1
    new_records = merged_df[merged_df['_merge'] == 'left_only']
    
    if not new_records.empty:
        new_records.drop(columns=['_merge'], inplace=True)

        # Keep only the columns from dataframe1
        new_records = new_records[[col + '_df1' if col != 'id' else col for col in columns]]
        # Rename the columns to remove the suffix
        new_records.columns = columns

        # generate insert queries
        insert_queries = generate_insert_queries(new_records, table_name) 
         
        # Insert new records
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    # Execute each insert query
                    # for query in insert_queries:
                    cur.execute(insert_queries)
                    conn.commit()  # Commit the transaction

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error inserting new records: {error}")

def process_updated_records(db_config, df_dbone, df_dbtwo, table_name, columns):
    # Merge both DataFrames on 'id'
    merged_df = df_dbone.merge(df_dbtwo, on="id", suffixes=("_df1", "_df2"), indicator=True)

    # Initialize the mask to False for all rows
    mask = pd.Series([False] * len(merged_df))

    # Update the mask for each column (excluding 'id')
    for col in columns:
        if col != 'id':
            # Use the pandas isnull() function to handle None/NaN comparisons
            mask |= ((merged_df[f'{col}_df1'] != merged_df[f'{col}_df2']) &
                 ~(pd.isnull(merged_df[f'{col}_df1']) & pd.isnull(merged_df[f'{col}_df2'])))

    # Apply the mask to filter the merged DataFrame
    updated_records = merged_df[mask]
    if not updated_records.empty:
        updated_records.drop(columns=['_merge'], inplace=True)
        
        # Keep only the columns from dataframe1
        updated_records = updated_records[[col + '_df1' if col != 'id' else col for col in columns]]
        # Rename the columns to remove the suffix
        updated_records.columns = columns

        # generate update queries
        update_queries = generate_update_queries(updated_records, table_name)

        # Update modified records
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    # Execute each insert query
                    for query in update_queries:
                        cur.execute(query)
                        conn.commit()  # Commit the transaction

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error updating existing records: {error}")

def process_data(parsed_query, columns):
    try:
        # Fetch a batch of records from dbone
        with psycopg2.connect(**dbone_config) as conn1:
            with conn1.cursor() as cursor1:
                cursor1.execute(parsed_query)
                records_dbone = cursor1.fetchall()

        with psycopg2.connect(**dbtwo_config) as conn2:
            with conn2.cursor() as cursor2:
                cursor2.execute(parsed_query)
                records_dbtwo = cursor2.fetchall()

        if not records_dbone:
            return -1  # No more records to process

        # Load the records into a pandas dataframe
        df_dbone = pd.DataFrame(records_dbone, columns=columns)
        df_dbtwo = pd.DataFrame(records_dbtwo, columns=columns)

        table_name = extract_table_name(parsed_query)
        process_new_records(dbtwo_config, df_dbone, df_dbtwo, table_name, columns)
        process_updated_records(dbtwo_config, df_dbone, df_dbtwo, table_name, columns)
        
        return records_dbone[-1][0]

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

def process_organizations(loaded_properties, batch_size):
    print("Processing organizations")
    # comma-separated organization IDs
    organization_ids = loaded_properties.get("organization_ids")
    # Split the comma-separated IDs into a list
    organization_ids_list = ",".join(organization_ids.split(","))

    # Organization List SQL query
    organization_query = loaded_properties.get("organization_query")

    columns = ["id", "type", "name", "first_name", "last_name", "email", "phone", "pwd_reset_required", "website", 
               "wallet", "password", "salt", "active_contract_id", "offering_pay_to_plant", "tree_validation_contract_id", 
               "logo_url", "map_name", "stakeholder_uuid"]

    last_processed_id = 0
    while last_processed_id > -1:
        parsed_query = organization_query % (last_processed_id, organization_ids_list, batch_size)
        # Process data & update the last processed ID
        last_processed_id = process_data(parsed_query, columns)
        if last_processed_id is None:
            print("entity: something went wrong so I could not get last processed record")
            break
        print(f"process organization iteration ended with last processed id: {last_processed_id}")
            
    print(f"process organization iterations done")


def process_planters(loaded_properties, batch_size):
    print("Processing planters")
    # comma-separated organization IDs
    organization_ids = loaded_properties.get("organization_ids")
    # Split the comma-separated IDs into a list
    organization_ids_list = ",".join(organization_ids.split(","))

    # Planter List SQL query
    planter_query = loaded_properties.get("planter_query")

    columns = ["id", "first_name", "last_name", "email", "organization", "phone", "pwd_reset_required", "image_url", 
               "person_id", "organization_id", "image_rotation", "gender", "grower_account_uuid"]

    last_processed_id = 0
    while last_processed_id > -1:
        parsed_query = planter_query % (last_processed_id, organization_ids_list, batch_size)
        # Process data & update the last processed ID
        last_processed_id = process_data(parsed_query, columns)
        if last_processed_id is None:
            print("planter: something went wrong so I could not get last processed record")
            break
        print(f"process planter iteration ended with last processed id: {last_processed_id}")
            
    print(f"process planter iterations done")  

def process_trees(loaded_properties, batch_size):
    print("Processing trees")
    # comma-separated organization IDs
    organization_ids = loaded_properties.get("organization_ids")
    # Split the comma-separated IDs into a list
    organization_ids_list = ",".join(organization_ids.split(","))

    # Planter List SQL query
    tree_query = loaded_properties.get("tree_query")

    columns = ["id", "time_created", "time_updated", "missing", "priority", "cause_of_death_id", "planter_id", 
               "primary_location_id", "settings_id", "override_settings_id", "dead", "photo_id", "image_url", 
               "certificate_id", "estimated_geometric_location", "lat", "lon", "gps_accuracy", "active", 
               "planter_photo_url", "planter_identifier", "device_id", "sequence", "note", "verified", "uuid", "approved", 
               "status", "cluster_regions_assigned", "species_id", "planting_organization_id", "payment_id", 
               "contract_id", "token_issued", "morphology", "age", "species", "capture_approval_tag", "rejection_reason", 
               "matching_hash", "device_identifier", "images", "domain_specific_data", "token_id", "name", "earnings_id", 
               "session_id"]

    last_processed_id = 0
    while last_processed_id > -1:
        parsed_query = tree_query % (last_processed_id, organization_ids_list, batch_size)
        
        # Process data & update the last processed ID
        last_processed_id = process_data(parsed_query, columns)
        if last_processed_id is None:
            print("tree: something went wrong so I could not get last processed record")
            break

        print(f"process tree iteration ended with last processed id: {last_processed_id}")
            
    print(f"process tree iterations done")

def scheduled_job():
    print("Migration job running")
    
    batch_size = loaded_properties.get("query_batch_size")
    process_organizations(loaded_properties, batch_size)
    process_planters(loaded_properties, batch_size)
    process_trees(loaded_properties, batch_size)
    

if __name__ == "__main__":
    print("Starting scheduler...")

    sched = BlockingScheduler()
    sched.add_job(scheduled_job, "interval", seconds=int(loaded_properties.get("job_interval_seconds")))
    sched.start()

