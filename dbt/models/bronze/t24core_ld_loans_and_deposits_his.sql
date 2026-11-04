-- Wave 0 | T24CORE.LD_LOANS_AND_DEPOSITS_HIS -> PG_BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS_HIS
-- 1:1 Raw Mirror (29 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/t24core_ld_loans_and_deposits_his.sql | schedule 30 0 * * *
{{ config(alias='t24core_ld_loans_and_deposits_his') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'T24CORE_LD_LOANS_AND_DEPOSITS_HIS' as SRC_STM_CODE
from {{ source('t24core', 'ld_loans_and_deposits_his') }} src
