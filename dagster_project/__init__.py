from dagster import repository

from jobs import sync_public_tables


@repository
def elt_repo():
    return [sync_public_tables]
