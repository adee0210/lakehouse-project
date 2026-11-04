-- Wave 3 | PG_GOLD.DIM_LOAN_AR (18 cột) – SCD TYPE 2 | Sheet 'Dimension Loan Arrangement' | Track 2 – Fresher 2
-- PG_SILVER.LOAN_AR a (SYS_EXP_DT IS NULL) LEFT JOIN PG_SILVER.LOAN_AR_PRFL b ON a.AR_ID = b.AR_ID AND b.SYS_EXP_DT IS NULL
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), a.AR_ID);
-- ETL_CHANGE_DATE = ngày thay đổi gần nhất của a hoặc b (GREATEST) để DIM_KEY đổi khi một trong hai bảng đổi.
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_loan_ar') }}

with src as (
    select
        {{ dim_key("greatest(" ~ etl_change_date('a') ~ ", coalesce(b.SYS_UDT_DT, b.SYS_EFF_DT, " ~ etl_change_date('a') ~ "))", 'a.AR_ID') }} as DIM_KEY,
        a.AR_ID,
        a.AR_CODE,
        a.FCC_AR_CODE,
        a.PPR_CTR_NBR,
        b.AR_LCS_TP_CODE,
        a.AR_PPS_GRP_CODE,
        a.AR_PPS_TP_CODE,
        a.ORIG_AMT_FCY,
        a.PD_CGY_CODE,
        b.PROM_PRGM   as PROM_CODE,
        b.PROM_PRGM_2 as PROM_CODE_2,
        a.TERM_CODE,
        a.VAL_DT,
        a.EFF_DT,
        a.MAT_DT,
        a.SYS_EFF_DT,
        cast(null as date) as SYS_EXP_DT
    from {{ ref('loan_ar') }} a
    left join {{ ref('loan_ar_prfl') }} b on a.AR_ID = b.AR_ID and b.SYS_EXP_DT is null
    where a.SYS_EXP_DT is null
)
{{ dim_scd2('AR_ID') }}
