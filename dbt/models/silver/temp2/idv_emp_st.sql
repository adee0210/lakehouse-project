-- Wave 2 | PG_SILVER.IDV_EMP_ST (8 cột) – SCD TYPE 2 | Sheet 'Individual Employment Status' | Track 1 – Fresher 1
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'SYS_EFF_DT'], alias='idv_emp_st') }}

with src as (
    select
        {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}   as CST_ID,
        cast(c.RECID as string)                    as CST_CODE,
        cast(c.EMPLOYMENT_STATUS as string)        as IDV_EMP_ST_CODE,
        cast(c.NET_MONTHLY_IN as decimal(25,5))    as ANUL_INCM,
        cast(c.OCCUPATION as string)               as OCP_CODE
    from {{ bz('t24core_customer') }} c
    where c.KHOI like '%INDIV%'
)
{{ scd2(['CST_ID']) }}
