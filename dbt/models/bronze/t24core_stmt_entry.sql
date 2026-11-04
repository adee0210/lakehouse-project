-- Wave 0 | T24CORE.STMT_ENTRY -> PG_BRONZE.T24CORE_STMT_ENTRY
-- 1:1 Raw Mirror (45 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_stmt_entry.sql | schedule 30 0 * * *
{{ config(alias='t24core_stmt_entry') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_STMT_ENTRY' as SRC_STM_CODE
from {{ source('t24core', 'stmt_entry') }} src
