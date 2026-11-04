-- Wave 2 | PG_SILVER.CST (22 cột) – SCD TYPE 1 | Sheet 'Customer' | Track 1 – Fresher 1
-- c = T24CORE_CUSTOMER, s = T24CORE_SECTOR (LEFT JOIN c.SECTOR = s.RECID)
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID'], alias='cst') }}

with src as (
    select
        {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}           as CST_ID,
        cast(c.RECID as string)                            as CST_CODE,
        cast(c.KHOI as string)                             as CST_GRP_CODE,
        cast(c.OPEN_COMPANY as string)                     as FRST_OU_CODE,
        cast(c.COMPANY_BOOK as string)                     as OU_CODE,
        cast(c.ACCOUNT_OFFICER as string)                  as MAKER_OFCR_CODE,
        cast(c.SECOND_OFFICER as string)                   as CHKER_OFCR_CODE,
        cast(c.REFERRED_BY as string)                      as REFR_CODE,
        cast(c.SECTOR as string)                           as SCTR_CODE,
        cast(c.SHORT_NAME as string)                       as SHRT_NM,
        cast(c.VN_FULL_NAME as string)                     as CST_NM,
        {{ t24_date('c.CONTACT_DATE') }}                   as OPN_DT,
        cast(c.ACCOUNT_OFFICER as string)                  as EMPE_CODE,
        cast(c.STREET as string)                           as STR_ADDR,
        cast(c.TARGET as string)                           as TRGT_CODE,
        {{ t24_date('coalesce(c.DATE_TIME, c.TXN_DATE)') }} as SRC_DT_TM,
        cast(c.MAU_HD as string)                           as CTR_TMPL_CODE,
        cast(c.MB_RM_BANCHEO as string)                    as CRS_SELL_RM_CODE,
        cast(s.CUSTOMER_TYPE as string)                    as CST_TP_CODE
    from {{ bz('t24core_customer') }} c
    left join {{ bz('t24core_sector') }} s on c.SECTOR = s.RECID
)
{{ scd1(['CST_ID']) }}
