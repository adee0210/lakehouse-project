-- Wave 0 | SOURCE.SAOKE_CRB -> PG_BRONZE.SOURCE_SAOKE_CRB
-- 1:1 Raw Mirror (17 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/source_saoke_crb.sql | schedule 30 0 * * *
{{ config(alias='source_saoke_crb') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'SOURCE_SAOKE_CRB' as SRC_STM_CODE
from {{ source('source', 'saoke_crb') }} src
