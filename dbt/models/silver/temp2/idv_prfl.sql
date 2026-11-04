-- Wave 2 | PG_SILVER.IDV_PRFL (27 cột) – SCD TYPE 2 | Sheet 'Individual Profile' | Track 1 – Fresher 1
-- Hồ sơ KHCN: WHERE c.KHOI LIKE '%INDIV%'
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'SYS_EFF_DT'], alias='idv_prfl') }}

with src as (
    select
        {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}                as CST_ID,
        cast(c.RECID as string)                                 as CST_CODE,
        cast(c.ACTIVE_STATUS as string)                         as CST_LCS_CODE,
        cast(c.AML_CHECK as string)                             as AML_CHK_F,
        cast(c.AML_RESULT as string)                            as AML_RSLT,
        cast(c.ASSET_CLASS as string)                           as CST_FNC_ST_CODE,
        cast(c.CO_CODE as string)                               as OU_CODE,
        cast(c.CUSTOMER_STATUS as string)                       as CST_ST_CODE,
        cast(c.INTERNET_BANKING_SERVICE as string)              as INN_BNKG_F,
        cast(c.KYC_CHANNEL as string)                           as KYC_CNL_CODE,
        cast(c.MA_BHXH as string)                               as VSS_CODE,
        cast(c.MOBILE_BANKING_SERVICE as string)                as MBL_BNKG_F,
        cast(c.NATIONALITY as string)                           as CTY_OF_CTZN_CODE,
        cast(c.NO_LIEN_DOI as string)                           as JNT_LBY_F,
        cast(coalesce(c.TAX_SERIAL, c.VAT_INV_SERIAL) as string) as TAX_IDENTN_NBR,
        cast(c.VIP_CUSTOMER as string)                          as VIP_F,
        cast(c.VN_GENDER as string)                             as GND,
        cast(c.CUS_IND_PARENT as string)                        as IDY_LV1_CL_CODE,
        cast(c.MB_INDUSTRY as string)                           as IDY_LV2_CL_CODE,
        {{ t24_date('coalesce(c.BIRTH_INCORP_DATE, c.BDATE_LEGAL)') }} as DOB,
        cast(c.FFB_TYPE as string)                              as VIP_TP_CODE,
        {{ t24_date('c.CU_EFF_DATE') }}                         as VIP_EFF_DT,
        {{ t24_date('c.VAT_INV_DATE') }}                        as BSN_RGST_EFF_DT,
        cast(c.MB_CR_STT as string)                             as MB_CR_ST
    from {{ bz('t24core_customer') }} c
    where c.KHOI like '%INDIV%'
)
{{ scd2(['CST_ID']) }}
