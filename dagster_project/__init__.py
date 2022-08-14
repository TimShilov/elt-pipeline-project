import os

from dagster import DefaultScheduleStatus, RunRequest, ScheduleEvaluationContext, job, op, repository, schedule
from dagster_snowflake import snowflake_resource
from dotenv import load_dotenv

load_dotenv('../.env')

source_database = 'AGENCY_DEVELOP_TIM'
destination_database = os.getenv('SNOWFLAKE_DATABASE')


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
def sync_public_tables():
    schema_created = ensure_publisher_schema()
    sync_crosses(schema_created)
    sync_network_keys(schema_created)


@schedule(job=sync_public_tables, cron_schedule="0 6 * * *", default_status=DefaultScheduleStatus.RUNNING)
def configurable_job_schedule(context: ScheduleEvaluationContext):
    scheduled_datetime = context.scheduled_execution_time.strftime("%m/%d/%Y, %H:%M:%S")
    return RunRequest(
        run_key="sync_public_tables",
        run_config={
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
        },
        tags={"scheduled_datetime": scheduled_datetime}
    )


@repository
def elt_repo():
    return [sync_public_tables, configurable_job_schedule]
