# Cow Swap Test

## Introduction
The project is split into three main sections:
* core set of library and services to spin up the scaffolding as well as fetch data from the various services,
* a flower and celery instance for job scheduling and results insights
* streamlit dashboard for presentation of the analysis

### Code layout

#### Core
* **config.py** - Contains all settings for env variables, validated with pydantic
* **db.py** - singleton patterns for database engines and sessions sync / async


#### Models
This contains the ORM models of the database, using the Base declaration found in core/db to init the general schema and relationships. 


#### Services
* **schema_init.py** - design pattern for inserting dimensions and creating scaffolding for a database.
___


## Setup
The project requires pipenv as means to manage dependencies. 
A folder for **logs** and **imports** / **exports** is needed as well. 
Not included in the push as it would be bad dev practice as it could contain secrets!.

You will need to connect to the database through a explorer of your choice, I like DBeaver. The details are in the ENV variables below are the same as those in the docker-compose file. 
A note that the database will be on localhost, the name below is the docker friendly name.

$ `
mkdir logs imports exports &&  
touch .env
`

# Environment Vars
Add the following settings into the .env file created in the command above.
The following vars are designed to work for the docker container, adjust if you wish to use a local instance of postgres as these are hunting for the docker network name and not the usual localhost/ip.
If **dev** is true it will drop and recreate all the tables within the database on every startup.

*  dev = True
*  debug_logs = False
*  db_string = 'postgresql//dev-user:password@postgres:5432/dev_db'
*  db_string_async = "postgresql+asyncpg://dev-user:password@postgres:5432/dev_db"
*  echo_sql = False
*  init_db = True
*  staging_schema = 'staging'
*  dw_schema = 'datawarehouse'
*  app_name='PysparkWatcher'
