-- Wave 0 | T24CORE.CUSTOMER -> PG_BRONZE.T24CORE_CUSTOMER
-- 1:1 Raw Mirror (63 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_customer.sql | schedule 30 0 * * *
{{ config(alias='t24core_customer') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_CUSTOMER' as SRC_STM_CODE
from {{ source('t24core', 'customer') }} src
