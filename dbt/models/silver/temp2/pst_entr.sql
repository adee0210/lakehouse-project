-- Wave 2 | PG_SILVER.PST_ENTR (52 cột) – TXN (giao dịch phát sinh trong ngày, phân vùng PST_DT) | Sheet 'Posting Entry' | Track 5
-- stmt: RECID NOT LIKE 'F%' AND nvl(COMMIT_ACTION, '_') <> 'D' AND BOOKING_DATE = ${ETL_DATE}
-- ld / pd: chỉ lấy bản ghi có DATE_TIME <= ${ETL_DATE}; pd khử trùng 1 dòng / hợp đồng để không nhân bản bút toán.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['PST_DT'],
          pre_hook="{{ delete_partition('PST_DT') }}", alias='pst_entr') }}

with stmt as (
    select
        case when s.OUR_REFERENCE like 'LD%' then s.OUR_REFERENCE
             when s.OUR_REFERENCE like 'PD%' then substr(s.OUR_REFERENCE, 3) end as contract_id,
        s.*
    from {{ bz('t24core_stmt_entry') }} s
    where s.RECID not like 'F%' and nvl(s.COMMIT_ACTION, '_') <> 'D' and {{ t24_date('s.BOOKING_DATE') }} = {{ etl_date() }}
),
ld as (
    select RECID, CATEGORY, CO_CODE from {{ bz('t24core_ld_loans_and_deposits') }}
    where {{ t24_date('DATE_TIME') }} <= {{ etl_date() }}
),
pd as (
    select CONTRACT_ID, CATEGORY, CO_CODE from (
        select substr(p.RECID, 3) as CONTRACT_ID, p.CATEGORY, p.CO_CODE,
               row_number() over (partition by substr(p.RECID, 3) order by p.PAY_TYPE desc) as rn
        from {{ bz('t24core_pd_payment_due_mv') }} p
        where nvl(p.FLAG_STATUS, '_') <> 'DELETE' and {{ t24_date('p.DATE_TIME') }} <= {{ etl_date() }}
    ) where rn = 1
)
select
    {{ fn_hash('T24_STMT_ENTRY', 'stmt.RECID') }}                       as PST_ENTR_ID,
    'T24_STMT_ENTRY'                                                    as SRC_STM_CODE,
    cast(stmt.RECID as string)                                          as PST_ENTR_NBR,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'stmt.OUR_REFERENCE') }}    as TXN_ID,
    cast(stmt.OUR_REFERENCE as string)                                  as TXN_NBR,
    cast(stmt.CRF_TYPE as string)                                       as ACG_CR_DB_F,
    cast(stmt.TRANSACTION_CODE as string)                               as TXN_CODE,
    {{ t24_date('stmt.BOOKING_DATE') }}                                 as PST_DT,
    cast(coalesce(stmt.AMOUNT_FCY, 0) as decimal(25,5))                 as PST_ENTR_AMT_FCY,
    cast(coalesce(stmt.AMOUNT_LCY, 0) as decimal(25,5))                 as PST_ENTR_AMT_LCY,
    cast(stmt.CURRENCY as string)                                       as CCY_CODE,
    {{ t24_date('stmt.VALUE_DATE') }}                                   as VAL_DT,
    cast(stmt.EXCHANGE_RATE as decimal(25,7))                           as EXG_RATE,
    cast(coalesce(ld.CO_CODE, pd.CO_CODE, stmt.COMPANY_CODE) as string) as OU_CODE,
    cast(stmt.ACCOUNT_NUMBER as string)                                 as AC_NBR,
    cast(stmt.contract_id as string)                                    as AR_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'stmt.contract_id') }}      as AR_ID,
    {{ fn_hash('T24_CUSTOMER', 'stmt.CUSTOMER_ID') }}                   as CST_ID,
    cast(stmt.CUSTOMER_ID as string)                                    as CST_NBR,
    cast(stmt.PRODUCT_CATEGORY as string)                               as ACG_PD_CGY,
    cast(stmt.NARRATIVE as string)                                      as PST_ENTR_DSC,
    cast(stmt.CONSOL_KEY as string)                                     as CONSOL_KEY,
    cast(stmt.SYSTEM_ID as string)                                      as MOD_CODE,
    case
        when stmt.TRANSACTION_CODE = '402' then 'LD_GN'
        when stmt.TRANSACTION_CODE in ('420', '423') and coalesce(ld.CATEGORY, pd.CATEGORY, stmt.CRF_PROD_CAT, '_') <> '21069' then 'LD_PR'
        when stmt.TRANSACTION_CODE in ('434', '424')
          or (stmt.TRANSACTION_CODE in ('420', '423') and coalesce(ld.CATEGORY, pd.CATEGORY, stmt.CRF_PROD_CAT) = '21069') then 'LD_IN'
        when stmt.TRANSACTION_CODE = '750' then 'PD_PR'
        when stmt.TRANSACTION_CODE = '751' then 'PD_IN'
        when stmt.TRANSACTION_CODE = '752' then 'PD_PE'
        when stmt.TRANSACTION_CODE = '753' then 'PD_PS'
    end                                                                 as PST_ENTR_CMPT,
    {{ t24_date('stmt.ACCOUNTING_DATE') }}                              as ACG_DT,
    case when stmt.ACCT_ID is not null then {{ fn_hash('T24_ACCOUNT', 'stmt.ACCT_ID') }} end as AC_ID,
    cast(stmt.BALANCE_TYPE as string)                                   as BAL_TP,
    cast(stmt.CARD_NUMBER as string)                                    as CARD_NBR,
    cast(stmt.COMMIT_ACTION as string)                                  as CMMT_ACTN,
    cast(stmt.COMMIT_TS as timestamp)                                   as CMMT_TMS,
    cast(stmt.CRF_PROD_CAT as string)                                   as CGY_PD_CODE,
    cast(stmt.CURR_NO as decimal(22,0))                                 as CRN_NBR,
    {{ t24_date('stmt.CYCLE_FORWARD') }}                                as CYC_FWD,
    {{ t24_date('stmt.DATE_TIME') }}                                    as DT_TM,
    cast(stmt.DEPARTMENT_CODE as string)                                as DEPT_CODE,
    cast(stmt.DRAWN_ACCOUNT as string)                                  as DRN_AC,
    cast(stmt.INTERNAL_CATEGORY as string)                              as INR_CGY,
    cast(stmt.LOCAL_REF as string)                                      as LCL_REFR,
    cast(stmt.ORIG_AMOUNT_LCY as decimal(25,5))                         as ORIG_AMT_LCY,
    cast(stmt.ORIGINAL_ACCT as string)                                  as ORIG_AC,
    cast(stmt.ORIGINAL_AMOUNT as decimal(25,5))                         as ORIG_AMT,
    cast(stmt.ORIGINAL_CCY as string)                                   as ORIG_CCY,
    cast(stmt.ORIGINAL_EXCH_RATE as decimal(25,7))                      as ORIG_EXG_RATE,
    cast(stmt.OUTSTANDING_BAL as decimal(25,5))                         as OTSND_BAL,
    cast(stmt.OVERRIDE as string)                                       as OVRD,
    cast(stmt.PROCESS_FORWARD as string)                                as PCS_FWD,
    cast(stmt.RECORD_STATUS as string)                                  as RCRD_ST,
    cast(stmt.REPAYMENT_AMT as decimal(25,5))                           as REPYMT_AMT,
    {{ t24_date('stmt.REPAYMENT_DATE') }}                               as REPYMT_DT,
    cast(stmt.REPAYMENT_TYPE as string)                                 as REPYMT_TP,
    cast(stmt.SUSPENSE_CATEGORY as string)                              as SUSP_CGY,
    {{ t24_date('stmt.SUSPNSE_VALUE_DATE') }}                           as SUSP_VAL_DT
from stmt
left join ld on ld.RECID = stmt.contract_id
left join pd on pd.CONTRACT_ID = stmt.contract_id
