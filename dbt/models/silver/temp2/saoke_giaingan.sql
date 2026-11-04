-- Wave 2 | PG_SILVER.Saoke_GIAINGAN – SNAPSHOT (Business Disbursement Statement)
-- Sheet 'Saoke_GIAINGAN': 73 cột. main = SOURCE_SAOKE_CRB (crb, APP_CODE LD/PD, TXN_DATE = ${ETL_DATE})
-- + SMY_STMT_DAILY (giao dịch giải ngân 402 trong ngày) + SAOKE_MVMT + CUSTOMER + COMPANY.
-- Grain: 1 dòng / dòng số dư crb (APP_ID x SBV_CODE). stmt/mvmt khử trùng 1 dòng / hợp đồng để không nhân bản số dư.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['ETL_DATE'],
          pre_hook="{{ delete_partition('ETL_DATE') }}", alias='saoke_giaingan') }}

with crb as (
    select * from {{ bz('source_saoke_crb') }}
    where APP_CODE in ('LD', 'PD') and {{ t24_date('TXN_DATE') }} = {{ etl_date() }}
),
stmt as (
    select * from (
        select s.*, row_number() over (partition by s.OUR_REFERENCE order by s.RECID desc) as rn
        from {{ bz('smy_stmt_daily') }} s
        where s.TRANSACTION_CODE = '402' and {{ t24_date('s.BOOKING_DATE') }} = {{ etl_date() }}
    ) where rn = 1
),
mvmt as (
    select * from (
        select m.*, row_number() over (partition by m.TRANS_REF order by m.SBV_CODE) as rn
        from {{ bz('source_saoke_mvmt') }} m
        where {{ t24_date('m.TXN_DATE') }} = {{ etl_date() }}
    ) where rn = 1
),
cus as (select * from {{ bz('t24core_customer') }}),
comp as (select * from {{ bz('t24core_company') }})
select
    TRIM(stmt.NARRATIVE) as NARRATIVE,
    COALESCE(CAST(stmt.ORIG_AMOUNT_LCY AS DECIMAL(25,5)), 0) as ORIG_AMOUNT_LCY,
    TRIM(stmt.ORIGINAL_ACCT) as ORIGINAL_ACCT,
    COALESCE(CAST(stmt.ORIGINAL_AMOUNT AS DECIMAL(25,5)), 0) as ORIGINAL_AMOUNT,
    TRIM(stmt.ORIGINAL_CCY) as ORIGINAL_CCY,
    COALESCE(CAST(stmt.ORIGINAL_EXCH_RATE AS DECIMAL(13,7)), 0) as ORIGINAL_EXCH_RATE,
    TRIM(stmt.OUR_REFERENCE) as OUR_REFERENCE,
    COALESCE(CAST(stmt.OUTSTANDING_BAL AS DECIMAL(25,5)), 0) as OUTSTANDING_BAL,
    TRIM(stmt.OVERRIDE) as OVERRIDE,
    TRIM(stmt.PROCESS_FORWARD) as PROCESS_FORWARD,
    {{ t24_date('stmt.PROCESSING_DATE') }} as PROCESSING_DATE,
    TRIM(stmt.PRODUCT_CATEGORY) as PRODUCT_CATEGORY,
    TRIM(stmt.RECID) as STMT_RECID,
    TRIM(stmt.RECORD_STATUS) as RECORD_STATUS,
    COALESCE(CAST(stmt.REPAYMENT_AMT AS DECIMAL(25,5)), 0) as REPAYMENT_AMT,
    {{ t24_date('stmt.REPAYMENT_DATE') }} as REPAYMENT_DATE,
    TRIM(stmt.REPAYMENT_TYPE) as REPAYMENT_TYPE,
    TRIM(stmt.STMT_ENTRY_ID) as STMT_ENTRY_ID,
    TRIM(stmt.STMT_NO) as STMT_NO,
    TRIM(stmt.SUSPENSE_CATEGORY) as SUSPENSE_CATEGORY,
    {{ t24_date('stmt.SUSPNSE_VALUE_DATE') }} as SUSPNSE_VALUE_DATE,
    TRIM(stmt.SYSTEM_ID) as SYSTEM_ID,
    TRIM(stmt.THEIR_REFERENCE) as THEIR_REFERENCE,
    TRIM(stmt.TRANS_REFERENCE) as TRANS_REFERENCE,
    TRIM(stmt.TRANSACTION_CODE) as TRANSACTION_CODE,
    {{ t24_date('stmt.TXN_DATE') }} as STMT_TXN_DATE,
    {{ t24_date('stmt.VALUE_DATE') }} as VALUE_DATE,
    TRIM(crb.ACC_FCC) as ACC_FCC,
    COALESCE(CAST(crb.AMOUNT_CUR AS DECIMAL(25,5)), 0) as AMOUNT_CUR,
    COALESCE(CAST(crb.AMOUNT_LCY AS DECIMAL(25,5)), 0) as AMOUNT_LCY,
    TRIM(crb.APP_CODE) as APP_CODE,
    TRIM(crb.APP_ID) as APP_ID,
    TRIM(cus.ASSET_CLASS) as ASSET_CLASS,
    TRIM(crb.ASSET_TYPE) as ASSET_TYPE,
    TRIM(crb.CATEG_CODE) as CATEG_CODE,
    TRIM(crb.CO_CODE) as CO_CODE,
    TRIM(crb.CURR) as CURR,
    TRIM(crb.CUS_ID) as CUS_ID,
    {{ t24_date('crb.DATE') }} as DATE,
    TRIM(crb.LOC_TERM) as LOC_TERM,
    {{ t24_date('crb.MAT_DATE') }} as MAT_DATE,
    TRIM(crb.SBV_CODE) as SBV_CODE,
    TRIM(crb.SUB_PRO) as SUB_PRO,
    {{ t24_date('crb.TXN_DATE') }} as TXN_DATE,
    {{ t24_date('crb.VAL_DATE') }} as VAL_DATE,
    TRIM(mvmt.SBV_CODE) as MVMT_SBV_CODE,
    TRIM(mvmt.TRANS_REF) as TRANS_REF,
    {{ t24_date('mvmt.TXN_DATE') }} as MVMT_TXN_DATE,
    TRIM(comp.COMPANY_NAME) as COMPANY_NAME,
    TRIM(comp.RECID) as COMP_RECID,
    TRIM(cus.ACCOUNT_OFFICER) as ACCOUNT_OFFICER,
    TRIM(cus.ACTIVE_STATUS) as ACTIVE_STATUS,
    TRIM(cus.AML_CHECK) as AML_CHECK,
    TRIM(cus.AML_RESULT) as AML_RESULT,
    {{ t24_date('cus.BDATE_LEGAL') }} as BDATE_LEGAL,
    {{ t24_date('cus.BDAY_DIRECTOR') }} as BDAY_DIRECTOR,
    {{ t24_date('cus.BIRTH_INCORP_DATE') }} as BIRTH_INCORP_DATE,
    TRIM(cus.CO_CODE) as CUS_CO_CODE,
    TRIM(cus.COMPANY_BOOK) as COMPANY_BOOK,
    {{ t24_date('cus.CONTACT_DATE') }} as CONTACT_DATE,
    TRIM(cus.CONTACT_PERSON) as CONTACT_PERSON,
    TRIM(cus.COUNTRY) as COUNTRY,
    {{ t24_date('cus.CU_EFF_DATE') }} as CU_EFF_DATE,
    TRIM(cus.CUS_IND_PARENT) as CUS_IND_PARENT,
    TRIM(cus.CUSTOMER_STATUS) as CUSTOMER_STATUS,
    {{ t24_date('cus.DATE_TIME') }} as DATE_TIME,
    TRIM(cus.EMAIL_ADDRESS) as EMAIL_ADDRESS,
    TRIM(cus.EMPLOYMENT_STATUS) as EMPLOYMENT_STATUS,
    TRIM(cus.ESTAB_LIC_CODE) as ESTAB_LIC_CODE,
    TRIM(cus.FFB_TYPE) as FFB_TYPE,
    TRIM(cus.GEN_DIR_PLC) as GEN_DIR_PLC,
    TRIM(cus.HOME_NUMBER) as HOME_NUMBER,
    TRIM(cus.INDUSTRY) as INDUSTRY,
    {{ etl_date() }} as ETL_DATE
from crb
left join stmt on crb.APP_ID = stmt.OUR_REFERENCE
left join mvmt on crb.APP_ID = mvmt.TRANS_REF
left join cus  on crb.CUS_ID = cus.RECID
left join comp on crb.CO_CODE = comp.RECID
