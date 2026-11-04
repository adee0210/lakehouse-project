-- Wave 1 | PG_SILVER.IP_ELC_ADR (7 cột) – SCD TYPE 2, unpivot 3 nhánh UNION ALL | Sheet 'Involved Party Electronic Addre'
-- Khoá tự nhiên tổ hợp (CST_ID, ELC_ADR_TP): EMAIL | HOME_NUMBER | MOBILE_NUMBER
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'ELC_ADR_TP', 'SYS_EFF_DT'], alias='ip_elc_adr') }}

with c as (select * from {{ bz('t24core_customer') }}),
src as (
    -- Nhánh 1: Email
    select {{ fn_hash('T24_CUSTOMER', 'c.RECID') }} as CST_ID, cast(c.RECID as string) as CST_CODE,
           'EMAIL' as ELC_ADR_TP, cast(c.EMAIL_ADDRESS as string) as ELC_ADR_VAL
    from c where c.EMAIL_ADDRESS is not null
    union all
    -- Nhánh 2: Điện thoại nhà riêng
    select {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}, cast(c.RECID as string), 'HOME_NUMBER', cast(c.HOME_NUMBER as string)
    from c where c.HOME_NUMBER is not null
    union all
    -- Nhánh 3: Điện thoại di động
    select {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}, cast(c.RECID as string), 'MOBILE_NUMBER', cast(c.MOBILE_NUMBER as string)
    from c where c.MOBILE_NUMBER is not null
)
{{ scd2(['CST_ID', 'ELC_ADR_TP']) }}
