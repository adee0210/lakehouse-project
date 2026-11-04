-- Wave 0 | T24CORE.CURRENCY_HIS -> PG_BRONZE.T24CORE_CURRENCY_HIS
-- 1:1 Raw Mirror (4 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_currency_his.sql | schedule 30 0 * * *
{{ config(alias='t24core_currency_his') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_CURRENCY_HIS' as SRC_STM_CODE
from {{ source('t24core', 'currency_his') }} src
