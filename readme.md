# Cow Swap Test


## Introduction
___
The project is split into the following sections:


## Project layout
___
#### Core
* **config.py** - Contains all settings for env variables, validated with pydantic
* **db.py** - singleton patterns for database engines and sessions sync / async
* **utils.py** - various tools to help with generating datetime's, id's and database schemas/objects dynamically
* **dune_analytics.py** - singleton pattern for dune client
* **coingecko.py** - various functions to authenticate and fetch target data

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
I have a feeling this is not actually re-running the query but fetching the existing results. 
But I ran out of credits and patience waiting for it to run in the queues. 
I understand the SDK & API enough to create, run and use the QueryBase and QueryParameter clients provided but they are engineered for paying customers in my honest opinion.

![Alt text](img/dune_api_button.png?raw=true "dune api button")

I have made my queries public on dune here: https://dune.com/workspace/u/botman_hotman/library/folders/creations?publicness=public
But just in case here are the respective queries I used, and you can add them to your dune ui, run them and then add the respective query id into the .env file.

###### **dune_analytics_init_query**

`select *
from
  cow_protocol_ethereum.trades
where
  buy_token = {{target_buy_token}}
  and sell_token = {{target_sell_token}}
  and block_date >= date_trunc('month', CURRENT_DATE)
  and block_date < CURRENT_DATE`



###### **dune_analytics_update_query**

`select *
from
cow_protocol_ethereum.trades
where
buy_token= {{target_buy_token}}
and sell_token = {{target_sell_token}}
and block_date = current_date - interval '1' DAY
`

I think the code for both functions would need to be update in servies/import_dune_data_to_staging.py to the following to make use of the query management better:

```
from dune_client.query import QueryBase, QueryParameter

target_query = QueryBase(
    query_id=query_id,
    params=[
        QueryParameter.text_type('target_buy_token', target_buy_token)
        , QueryParameter.text_type('target_sell_token', target_sell_token)
    ]
)

query_result: pd.DataFrame = dune.run_query_dataframe(target_query)
```

## Docker
___