from dagster import repository

from jobs import sync_public_tables


@repository
def dagster_project():
    return [sync_public_tables]
