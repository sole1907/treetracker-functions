Run the following command to create a virtual environment (replace myenv with your preferred name):
`python3 -m venv myenv`

## Activate the virtual environment:

#### On Windows: `myenv\Scripts\activate`

#### On macOS/Linux: `source myenv/bin/activate`

## Install requirements

`pip install -r requirements.txt`

## Create an Environment Variable File

Create a .env file in the project directory to store environment variables.
Open the .env file and add the following key-value configs, update the values.

```
SRC_DB_HOST=localhost
SRC_DB_PORT=5432
SRC_DB_NAME=mydatabase
SRC_DB_USER=myuser
SRC_DB_PASSWORD=mypassword

DEST_DB_HOST=localhost
DEST_DB_PORT=5432
DEST_DB_NAME=mydatabase
DEST_DB_USER=myuser
DEST_DB_PASSWORD=mypassword
```

## Update config.properties file

```
# here job is scheduled to run every 24 hours, set value to preferred frequency
job_interval_seconds=86400
# set batch size for each run, 1000 here means a limit of 1000 records per batch of query
query_batch_size=1000
# list of organization_ids separated by commas
organization_ids=15,1,13,14,2,3,4,5,6,30,31,32,11

#queries ...
list of queries being run for data migration
```

## Activate Environment Variables:

Activate the environment variables by sourcing the .env file in your terminal: `source .env`

## Run Application:

Run the application using: `python migration_job.py`
