-- Wave 3 | PG_GOLD.DIM_CCY (7 cột) – SCD TYPE 2 | Sheet 'Dimension Currency' | Track 3 – Fresher 3
-- Nguồn: PG_SILVER.CCY cur (bản ghi hiệu lực)
-- DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), cur.CCY_ID), ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_ccy') }}

with src as (
    select
        {{ dim_key(etl_change_date('cur'), 'cur.CCY_ID') }} as DIM_KEY,
        cur.CCY_ID             as CCY_ID,
        cur.CCY_CODE           as CCY_CODE,
        cur.CCY_NM             as CCY_NM,
        cur.NUMERIC_CCY_CODE   as NUMERIC_CCY_CODE,
        cur.SYS_EFF_DT          as SYS_EFF_DT,
        cast(null as date)       as SYS_EXP_DT
    from {{ ref('ccy') }} cur
    where cur.SYS_EXP_DT is null
)
{{ dim_scd2('CCY_ID') }}
