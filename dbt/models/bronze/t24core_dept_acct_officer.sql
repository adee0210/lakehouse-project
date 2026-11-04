-- Wave 0 | T24CORE.DEPT_ACCT_OFFICER -> PG_BRONZE.T24CORE_DEPT_ACCT_OFFICER
-- 1:1 Raw Mirror (2 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_dept_acct_officer.sql | schedule 30 0 * * *
{{ config(alias='t24core_dept_acct_officer') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_DEPT_ACCT_OFFICER' as SRC_STM_CODE
from {{ source('t24core', 'dept_acct_officer') }} src
