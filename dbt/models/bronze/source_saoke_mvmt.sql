-- Wave 0 | SOURCE.SAOKE_MVMT -> PG_BRONZE.SOURCE_SAOKE_MVMT
-- 1:1 Raw Mirror (3 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/source_saoke_mvmt.sql | schedule 30 0 * * *
{{ config(alias='source_saoke_mvmt') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'SOURCE_SAOKE_MVMT' as SRC_STM_CODE
from {{ source('source', 'saoke_mvmt') }} src
