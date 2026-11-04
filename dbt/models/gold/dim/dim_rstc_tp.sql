-- Wave 3 | PG_GOLD.DIM_RSTC_TP (6 cột) – SCD TYPE 2 | Sheet 'Dimension Loan Restructure Type' | Track 3 – Fresher 3
-- Nguồn: PG_SILVER.CV cv (bản ghi hiệu lực) – lọc CL_SCM_CODE = 'LOAN_RSTC_TP'
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), cv.CL_ID), ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_rstc_tp') }}

with src as (
    select
        {{ dim_key(etl_change_date('cv'), 'cv.CL_ID') }} as DIM_KEY,
        cv.CL_ID              as RSTC_TP_ID,
        cv.CL_CODE            as RSTC_TP_CODE,
        cv.CL_DSC             as RSTC_TP_DSC,
        cv.SYS_EFF_DT          as SYS_EFF_DT,
        cast(null as date)       as SYS_EXP_DT
    from {{ ref('cv') }} cv
    where cv.SYS_EXP_DT is null and cv.CL_SCM_CODE = 'LOAN_RSTC_TP'
)
{{ dim_scd2('RSTC_TP_ID') }}
