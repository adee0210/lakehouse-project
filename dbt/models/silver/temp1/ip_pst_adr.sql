-- Wave 1 | PG_SILVER.IP_PST_ADR (10 cột) – SCD TYPE 2, unpivot 2 nhánh UNION ALL | Sheet 'Involved Party Postal Address'
-- Khoá tự nhiên tổ hợp (CST_ID, PST_ADR_TP): CURRENT ADDRESS | PERMANENT ADDRESS
-- Ghép chuỗi kiểu Oracle (a || ',' || b): NULL được coi như chuỗi rỗng.
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'PST_ADR_TP', 'SYS_EFF_DT'], alias='ip_pst_adr') }}

with c as (select * from {{ bz('t24core_customer') }}),
src as (
    select {{ fn_hash('T24_CUSTOMER', 'c.RECID') }} as CST_ID, cast(c.RECID as string) as CST_CODE,
           'CURRENT ADDRESS' as PST_ADR_TP,
           concat(nvl(c.VN_STREET, ''), ',', nvl(c.VN_TOWN, ''), ',', nvl(c.COUNTRY, '')) as PST_ADR_VAL,
           cast(c.COUNTRY as string) as CTY_NM, cast(c.VN_TOWN as string) as PROV_NM, cast(c.VN_STREET as string) as STR_NM
    from c
    union all
    select {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}, cast(c.RECID as string),
           'PERMANENT ADDRESS',
           concat(nvl(c.RESID_PROVINCE, ''), ',', nvl(c.RESIDENCE, '')),
           cast(c.RESIDENCE as string), cast(c.RESID_PROVINCE as string), cast(c.STREET as string)
    from c
)
{{ scd2(['CST_ID', 'PST_ADR_TP']) }}
