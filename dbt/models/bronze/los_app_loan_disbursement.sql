-- Wave 0 | LOS_APP.LOAN_DISBURSEMENT@TO_LOS -> PG_BRONZE.LOS_APP_LOAN_DISBURSEMENT
-- 1:1 Raw Mirror (5 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/los_app_loan_disbursement.sql | schedule 30 0 * * *
{{ config(alias='los_app_loan_disbursement') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'LOS_APP_LOAN_DISBURSEMENT' as SRC_STM_CODE
from {{ source('los_app', 'loan_disbursement') }} src
