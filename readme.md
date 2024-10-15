# Cow Swap Test


## Introduction
___
This project is designed to be a self-contained ETL system, it utilises the following services to extract, transform, save, and monitor all pipeline jobs:
* Postgres - database
* Celery - an asynchronous task runner and scheduler 
* Flower - a UI to monitor the celery jobs and outputs
* RedisCache - in memory event broker and cache to support celery and flower
* Streamlit - A dash-boarding framework
* Dune Analytics client - an sdk to interface with a on-chain postgres database to ETL the various data
* Coin Gecko - a crypto data aggregator
* Docker - microservice orchestration and containerisation

The system initialises with the current months data pulled from Dune and Coin Gecko, it will pull WETH-USDC from dune and ethereum and usdc price data from coingecko 
and insert it into the staging section of the database. After this has completed, all views and aggregations are created within the database. 
The celery worker has all cronjobs scheduled for 9AM UTC will sit with a 'beat' waiting for the time. 

## Project layout
___
#### Core
* **config.py** - Contains all settings for env variables, validated with pydantic
* **db.py** - singleton patterns for database engines and sessions sync / async
* **utils.py** - various tools to help with generating datetime's, id's and database schemas/objects dynamically
* **dune_analytics.py** - singleton pattern for dune client
* **coingecko.py** - a wrapper around the api with various functions to authenticate and fetch target data

#### Models
* This contains the ORM models of the database, using the Base declaration found in core/db to init the general schema and relationships. If we needed to create complex relations/schemas we could define them here making them version control friendly and migratable.


#### Services
* **import_coingecko_to_staging** - init / update functions to fetch and process coin gecko data into staging tables
* **import_dune_data_to_staging** - init / update functions to fetch and process dune data into staging tables
___

#### Main
* **main.py** - init's the database and fetches all starting data from the start of the month.
* **app.py** - one page streamlit dashboard as a presentation layer, typically I would feed this with an API for scalability like FastAPI, but not in scope.
* **celery_worker** - interface to start the celery scheduler and flower job watcher. Allows for visibility of jobs ran. 


# Setup
___
The project requires pipenv as means to manage dependencies. 
A folder for **logs** is needed as well. 
The commands below will generate the base files which are not included in the push as it would be bad dev practice as it could contain secrets!.

$ `
mkdir logs && touch .env
`

You will need to connect to the database through a explorer of your choice, I like DBeaver. The details are in the ENV variables below are the same as those in the docker-compose file. 
_**A note that the database will be on localhost, the name below is the docker friendly name.**_

## Environment Vars
___
Add the following settings into the .env file created in the command above.
The following vars are designed to work for the docker container, 
adjust if you wish to use a local instance of postgres as these are hunting for the docker network name and not the usual localhost/ip.
If **dev** is true it will drop and recreate all the tables within the database on every startup.
Be sure to insert your own dune analytics and coingecko api key.


* dev = True 
* debug_logs = False
* db_string = 'postgresql://dev-user:password@postgres:5432/dev_db' 
* db_string_async = "postgresql+asyncpg://dev-user:password@postgres:5432/dev_db"
* celery_broker = 'redis://rd01:6379/0' 
* celery_backend ='redis://rd01:6379/0'
* echo_sql = False 
* init_db = True 
* staging_schema = 'staging' 
* dw_schema = 'data_warehouse' 
* app_name='CowSwapTest'
* coingecko_api_key = '' 
* coingecko_api_url = 'https://api.coingecko.com/api/v3'
* dune_analytics_key = '' 
* dune_analytics_url = "https://api.dune.com"
* dune_analytics_init_query_id = 4158553
* dune_analytics_update_query_id = 4148595

## Dune Analytics
___
The free tier of dune analytics doesn't allow for the management of sql queries through the API (sad times).
The best workaround I found was to create them in the dune UI and then fetch the query id using the button below, 
and add it to the .env file for the two respective queries I wanted to use for the pipeline. 

![Alt text](img/dune_api_button.png?raw=true "dune api button")


A query can either be re-ran and fresh data will be pulled, or existing results can be fetched. 
In the dune ui a query can be scheduled to run which would help with the latter.

To fetch existing results the code would need to be changed. This code just reruns the query so fresh data can be pulled. 

I have made my queries public on dune here: https://dune.com/workspace/u/botman_hotman/library/folders/creations
But just in case here are the respective queries I used, and you can add them to your dune ui, 
run them and then add the respective query id into the .env file. I have left the ID's of my queries in the .env file of the zipped. 

###### **dune_analytics_init**
`
select *
from
  cow_protocol_ethereum.trades
where
  buy_token = '{{target_buy_token}}'
  and sell_token = '{{target_sell_token}}'
  and block_date >= date_trunc('month', CURRENT_DATE)
  and block_date < CURRENT_DATE
`

###### **dune_analytics_update**
This query will fetch data from the start of the month, this could be adjusted with the parameters, but for demo sakes this is suffice.
`
select *
from
cow_protocol_ethereum.trades
where
buy_token= '{{target_buy_token}}'
and sell_token = '{{target_sell_token}}'
and block_date = current_date - interval '1' DAY
`

## Docker
___
To start all the required services enter the root of the directory and enter the following. 

`docker-compose up`

It will kick off the following microservices: 
* postgres server 
* init service to fetch starter data
* redis server
* a celery worker/beat, 
* flower instance as a UI for the celery worker
* streamlit dashboard 

with the command above you should see the following happen
![Alt text](img/docker-compose.png?raw=true "dune api button")

![Alt text](img/docker up.png?raw=true "dune api button")

![Alt text](img/docker output.png?raw=true "dune api button")


## Postgres
___

If all has ran successfully you should see a staging area with our two target systems data tables that builds the foundation of the analysis. 
Each table generates a primary key ID column of type UUID, which is created by stringifying all items in a row and then taking a hash of that string 
and turning it into a UUID. Helps combat duplicates. The is also the data warehouse schema where I have created views that are used in the streamlit dashboard. 

![Alt text](img/postgres ready.png?raw=true "dune api button")


## Streamlit Dash & Flower

---
* The status/outcome of the jobs can be seen through Flower, located @ http://0.0.0.0:5555/
![Alt text](img/flowe.png?raw=true "dune api button")



* A streamlit dashboard, to present the findings, can be found @ http://localhost:8501/
![Alt text](img/streamlit.png?raw=true "dune api button")

# Final Thoughts
___
**Source data** - I think more time should be spent with Dune, aggregations, transformation and filtering at the source will speed up the whole pipeline. 
Also, exploring the option of using more that one data aggregator to distribute the average across the market. As well as exploring more data sources within dune itself.

**Notifications** - certain processes could be ran in try/except blocks and should an error be caught an email/teams/error log watcher message could be sent for visability

**Database Management** - The staging tables could use some indexing / clustering / partitioning as the data will become very large as the service is expanded into 
other coins.

**Version Control Queries (Dune)** - Creating a pipeline that creates all required queries to dune as well as storing the query_id's and metadata in a local postgres database for 
easier deployment into the pipeline. 

**Scaling/Cloud** - This services could be deployed into kubernetes for maximum scalability with a cloud sql database should we need more compute.  

**Analytics API** - Could create a fast API to serve the results for custom dashboarding or feeds into other systems. As well as manually triggering jobs by hitting target endpoints. 