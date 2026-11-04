-- Wave 0 | T24CORE.PD_PAYMENT_DUE_MV -> PG_BRONZE.T24CORE_PD_PAYMENT_DUE_MV
-- 1:1 Raw Mirror (23 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_pd_payment_due_mv.sql | schedule 30 0 * * *
{{ config(alias='t24core_pd_payment_due_mv') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_PD_PAYMENT_DUE_MV' as SRC_STM_CODE
from {{ source('t24core', 'pd_payment_due_mv') }} src
