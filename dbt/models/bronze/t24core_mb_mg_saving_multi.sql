-- Wave 0 | T24CORE.MB_MG_SAVING_MULTI -> PG_BRONZE.T24CORE_MB_MG_SAVING_MULTI
-- 1:1 Raw Mirror (3 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_mb_mg_saving_multi.sql | schedule 30 0 * * *
{{ config(alias='t24core_mb_mg_saving_multi') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_MB_MG_SAVING_MULTI' as SRC_STM_CODE
from {{ source('t24core', 'mb_mg_saving_multi') }} src
