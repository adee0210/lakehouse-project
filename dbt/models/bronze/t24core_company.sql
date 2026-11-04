-- Wave 0 | T24CORE.COMPANY -> PG_BRONZE.T24CORE_COMPANY
-- 1:1 Raw Mirror (2 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_company.sql | schedule 30 0 * * *
{{ config(alias='t24core_company') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_COMPANY' as SRC_STM_CODE
from {{ source('t24core', 'company') }} src
