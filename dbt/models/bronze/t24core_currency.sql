-- Wave 0 | T24CORE.CURRENCY -> PG_BRONZE.T24CORE_CURRENCY
-- 1:1 Raw Mirror (6 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_currency.sql | schedule 30 0 * * *
{{ config(alias='t24core_currency') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_CURRENCY' as SRC_STM_CODE
from {{ source('t24core', 'currency') }} src
