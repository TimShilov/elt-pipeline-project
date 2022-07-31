# elt-pipeline-project

## Prepequisites

* dbt installed
* Snowflake resources:
    * WAREHOUSE, STORAGE INTEGRATION and an external STAGE
    * USER and DATABASE
    * Snowflake ROLE with following permissions:
        * ALL grant on DATABASE
        * USAGE grant on WAREHOUSE
        * USAGE grant on STORAGE INTEGRATION
    * Custom FILE FORMAT for JSON
        ```sql
        create or replace file format json_format type = 'json';
