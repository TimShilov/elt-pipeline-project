# elt-pipeline-project

This is my graduation project for [Otus Data Engineer course](https://otus.ru/lessons/data-engineer/).
The purpose is to build a data pipeline that takes data from Google Cloud Storage bucket, loads and transforms it into Snowflake data warehouse.

## Prepequisites

- dbt installed
- Snowflake resources:
  - WAREHOUSE, STORAGE INTEGRATION and an external STAGE
  - USER and DATABASE
  - Snowflake ROLE with following permissions:
    - ALL grant on DATABASE
    - USAGE grant on WAREHOUSE
    - USAGE grant on STORAGE INTEGRATION
  - Custom FILE FORMAT for JSON
    ```sql
    create or replace file format json_format type = 'json';
    ```
