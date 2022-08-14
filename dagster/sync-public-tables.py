import os

from dagster import job, op
from dagster_snowflake import snowflake_resource
from dotenv import load_dotenv

load_dotenv('../.env')

source_database = 'AGENCY_DEVELOP_TIM'
destination_database = os.getenv('SNOWFLAKE_DATABASE')


@op(required_resource_keys={'snowflake'})
def sync_crosses(context):
    context.resources.snowflake.execute_query(f"""
        CREATE OR REPLACE TABLE {destination_database}.PUBLIC.CROSSES
            CLONE {source_database}.PUBLIC.CROSSES;
    """)


@op(required_resource_keys={'snowflake'})
def sync_network_keys(context):
    context.resources.snowflake.execute_query(f"""
        CREATE OR REPLACE TABLE {destination_database}.PUBLIC.NETWORK_KEY
            CLONE {source_database}.PUBLIC.NETWORK_KEY;
    """)


@job(resource_defs={'snowflake': snowflake_resource})
def my_snowflake_job():
    sync_crosses()
    sync_network_keys()
