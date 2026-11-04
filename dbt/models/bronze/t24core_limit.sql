-- Wave 0 | T24CORE.LIMIT -> PG_BRONZE.T24CORE_LIMIT
-- 1:1 Raw Mirror (6 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_limit.sql | schedule 30 0 * * *
{{ config(alias='t24core_limit') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_LIMIT' as SRC_STM_CODE
from {{ source('t24core', 'limit') }} src
