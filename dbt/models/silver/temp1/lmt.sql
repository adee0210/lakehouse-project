-- Wave 1 | PG_SILVER.LMT (11 cột) – SCD TYPE 1 | Sheet 'Limit' | Track 3 – Fresher 3
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['LMT_ID'], alias='lmt') }}

with src as (
    select
        {{ fn_hash('T24_LIMIT', 'l.RECID') }}            as LMT_ID,
        cast(l.RECID as string)                          as LMT_CODE,
        cast(l.LIMIT_PRODUCT as string)                  as LMT_DSC,
        cast(l.INTERNAL_AMOUNT as decimal(25,5))         as INR_LMT_AMT,
        cast(l.MAXIMUM_TOTAL as decimal(25,5))           as MAX_TOT_LMT,
        {{ t24_date('l.ONLINE_LIMIT_DATE') }}            as ONL_LMT_DT,
        {{ t24_date('l.EXPIRY_DATE') }}                  as LMT_EXP_DT,
        'T24_LIMIT'                                      as SRC_STM_CODE
    from {{ bz('t24core_limit') }} l
)
{{ scd1(['LMT_ID']) }}
