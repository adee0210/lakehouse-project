-- Wave 2 | PG_SILVER.ORG_PRFL (30 cột) – SCD TYPE 2 | Sheet 'Organization Profile' | Track 1 – Fresher 1
-- Hồ sơ KHDN: WHERE c.KHOI NOT LIKE '%INDIV%'
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CST_ID', 'SYS_EFF_DT'], alias='org_prfl') }}

with src as (
    select
        {{ fn_hash('T24_CUSTOMER', 'c.RECID') }}                as CST_ID,
        cast(c.RECID as string)                                 as CST_CODE,
        cast(c.ACTIVE_STATUS as string)                         as CST_LCS_CODE,
        cast(c.ASSET_CLASS as string)                           as CST_FNC_ST_CODE,
        {{ t24_date('c.BIRTH_INCORP_DATE') }}                   as ESTB_DT,
        cast(c.CO_CODE as string)                               as OU_CODE,
        cast(c.CUSTOMER_STATUS as string)                       as CST_ST_CODE,
        cast(c.INDUSTRY as string)                              as IDY_CL_CODE,
        cast(c.INTERNET_BANKING_SERVICE as string)              as INN_BNKG_F,
        cast(c.MOBILE_BANKING_SERVICE as string)                as MBL_BNKG_F,
        cast(c.NO_LIEN_DOI as string)                           as JNT_LBY_F,
        cast(coalesce(c.TAX_SERIAL, c.VAT_INV_SERIAL) as string) as TAX_IDENTN_NBR,
        cast(c.VIP_CUSTOMER as string)                          as VIP_F,
        cast(c.CUS_IND_PARENT as string)                        as IDY_LV1_CL_CODE,
        cast(c.MB_INDUSTRY as string)                           as IDY_LV2_CL_CODE,
        cast(c.ESTAB_LIC_CODE as string)                        as BSN_RGST_NBR,
        {{ t24_date('c.VAT_INV_DATE') }}                        as BSN_RGST_EFF_DT,
        cast(c.FFB_TYPE as string)                              as VIP_TP_CODE,
        {{ t24_date('c.CU_EFF_DATE') }}                         as VIP_EFF_DT,
        cast(c.KYC_CHANNEL as string)                           as KYC_CNL_CODE,
        cast(c.GEN_DIR_PLC as string)                           as GEN_DIR_PLC,
        {{ t24_date('c.BDAY_DIRECTOR') }}                       as DIR_DOB,
        cast(c.CONTACT_PERSON as string)                        as CNTCT_PRSN,
        cast(c.PHONE_CONT_PER as string)                        as CNTCT_PRSN_PHN,
        cast(c.AML_RESULT as string)                            as AML_RSLT,
        cast(c.AML_CHECK as string)                             as AML_CHK_F,
        cast(c.MB_CR_STT as string)                             as MB_CR_ST
    from {{ bz('t24core_customer') }} c
    where c.KHOI not like '%INDIV%'
)
{{ scd2(['CST_ID']) }}
