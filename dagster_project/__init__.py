import os

from dagster import DefaultScheduleStatus, RunRequest, ScheduleEvaluationContext, job, op, repository, schedule
from dagster_snowflake import snowflake_resource
from dotenv import load_dotenv

load_dotenv('../.env')

source_database = 'AGENCY_DEVELOP_TIM'
destination_database = os.getenv('SNOWFLAKE_DATABASE')

default_run_config = {
    "resources": {
        "snowflake": {
            "config": {
                "account": {"env": "SNOWFLAKE_ACCOUNT"},
                "user": {"env": "SNOWFLAKE_USERNAME"},
                "password": {"env": "SNOWFLAKE_PASSWORD"},
                "database": {"env": "SNOWFLAKE_DATABASE"},
                "schema": "DBT",
                "warehouse": {"env": "SNOWFLAKE_WAREHOUSE"}
            }
        }
    }
}


@op(required_resource_keys={'snowflake'})
def ensure_publisher_schema(context):
    schema_to_create = f"{destination_database}.PUBLIC";
    context.resources.snowflake.execute_query(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_to_create};
    """)
    return schema_to_create


@op(required_resource_keys={'snowflake'})
def sync_crosses(context, schema_created):
    context.resources.snowflake.execute_query(f"""
        CREATE OR REPLACE TABLE {schema_created}.CROSSES
            CLONE {source_database}.PUBLIC.CROSSES;
    """)


@op(required_resource_keys={'snowflake'})
def sync_network_keys(context, schema_created):
    context.resources.snowflake.execute_query(f"""
        CREATE OR REPLACE TABLE {schema_created}.NETWORK_KEY
            CLONE {source_database}.PUBLIC.NETWORK_KEY;
    """)


@job(resource_defs={'snowflake': snowflake_resource})
def sync_crosses_table():
    schema_created = ensure_publisher_schema()
    sync_crosses(schema_created)


@job(resource_defs={'snowflake': snowflake_resource})
def sync_network_keys_table():
    schema_created = ensure_publisher_schema()
    sync_network_keys(schema_created)


@schedule(job=sync_crosses_table, cron_schedule="0 6 * * *", default_status=DefaultScheduleStatus.RUNNING)
def sync_crosses_schedule(context: ScheduleEvaluationContext):
    scheduled_datetime = context.scheduled_execution_time.strftime("%m/%d/%Y, %H:%M:%S")

    return RunRequest(
        run_key="sync_crosses_table",
        run_config=default_run_config,
        tags={"scheduled_datetime": scheduled_datetime}
    )


@schedule(job=sync_network_keys_table, cron_schedule="0 */1 * * *", default_status=DefaultScheduleStatus.RUNNING)
def sync_network_keys_schedule(context: ScheduleEvaluationContext):
    scheduled_datetime = context.scheduled_execution_time.strftime("%m/%d/%Y, %H:%M:%S")

    return RunRequest(
        run_key="sync_network_keys_table",
        run_config=default_run_config,
        tags={"scheduled_datetime": scheduled_datetime}
    )


@repository
def elt_repo():
    return [sync_crosses_table, sync_network_keys_table, sync_crosses_schedule, sync_network_keys_schedule]
