-- Wave 0 | FLEXBO.PGBLD_CONTRACT_UDFIELD_HIST -> PG_BRONZE.FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST
-- 1:1 Raw Mirror (4 cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/flexbo_pgbld_contract_udfield_hist.sql | schedule 30 0 * * *
{{ config(alias='flexbo_pgbld_contract_udfield_hist') }}

select
    src.*,
    {{ etl_date() }} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    'FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST' as SRC_STM_CODE
from {{ source('flexbo', 'pgbld_contract_udfield_hist') }} src
