-- Wave 1 | PG_SILVER.IP_ALT_IDENTN (10 cột) – SCD TYPE 2 | Sheet 'Involved Party Alternative Iden' | Track 1 – Fresher 1
-- Giấy tờ tuỳ thân khách hàng: WHERE c.NATIONAL_ID IS NOT NULL
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'SYS_EFF_DT'], alias='ip_alt_identn') }}

with src as (
    select
        {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}  as CST_ID,
        cast(c.RECID as string)                   as CST_CODE,
        cast(c.NAT_ID_TYPE as string)             as IDENTN_TP_CODE,
        {{ t24_date('c.NAT_ID_ISS_DATE') }}       as IDENTN_ISSU_DT,
        {{ t24_date('c.NAT_ID_EXP_DATE') }}       as IDENTN_EXPIRY_DT,
        cast(c.NAT_ID_ISS_PLC as string)          as ISSU_PLC_CODE,
        cast(c.NATIONAL_ID as string)             as IDENTN_NBR
    from {{ bz('t24core_customer') }} c
    where c.NATIONAL_ID is not null
)
{{ scd2(['CST_ID']) }}
