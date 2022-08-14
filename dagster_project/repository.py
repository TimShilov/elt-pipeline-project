from dagster import repository

from dagster_project import jobs


@repository
def dagster_project():
    return jobs
