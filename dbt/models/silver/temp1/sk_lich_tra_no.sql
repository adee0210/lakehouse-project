-- Wave 1 | PG_SILVER.sk_lich_tra_no – SNAPSHOT (Silver Temp 1 - Technical Schedule Staging)
-- Sheet 'sk_lich_tra_no' + Map_SRC2TRG: 117 cột, main = T24CORE_PD_PAYMENT_DUE_MV (pdmv), 10 bảng phụ.
-- Grain: 1 dòng / dòng pdmv x bút toán stmt phát sinh trong ngày của hợp đồng.
-- Các bảng lịch sử (pdhmv, pdh, ldh, sch) lấy bản ghi mới nhất để tránh fan-out N x M.
-- Cột ETL_DATE bổ sung làm khoá phân vùng snapshot (ghi đè theo ngày chạy).
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['ETL_DATE'],
          pre_hook="{{ delete_partition('ETL_DATE') }}", alias='sk_lich_tra_no') }}

with pdmv as (
    select * from {{ bz('t24core_pd_payment_due_mv') }} where nvl(FLAG_STATUS, '_') <> 'DELETE'
),
pd as (select * from {{ bz('t24core_pd_payment_due') }}),
pdhmv as (
    select * from (
        select h.*, row_number() over (partition by h.RECID, h.PAY_TYPE order by cast(h.CURR_NO as int) desc, h.DATE_TIME desc) as rn
        from {{ bz('t24core_pd_payment_due_his_mv') }} h
        where h.FLAG_STATUS = 'LIVE' and cast(h.PAY_AMT_OUTS as decimal(25,5)) > 0
    ) where rn = 1
),
pdh as (
    select * from (
        select h.*, row_number() over (partition by h.RECID order by cast(h.CURR_NO as int) desc) as rn
        from {{ bz('t24core_pd_payment_due_his') }} h
    ) where rn = 1
),
sch as (
    select * from (
        select s.*, row_number() over (partition by s.CONTRACT_ID order by {{ t24_date('s.PREV_SCHED_DATE') }} desc) as rn
        from {{ bz('t24core_ld_schedule_define_mv') }} s
        where {{ t24_date('s.PREV_SCHED_DATE') }} <= {{ etl_date() }}
    ) where rn = 1
),
ld as (select * from {{ bz('t24core_ld_loans_and_deposits') }}),
ldh as (
    select * from (
        select h.*, row_number() over (partition by substr(h.RECID, 1, instr(h.RECID, ';') - 1) order by cast(h.CURR_NO as int) desc) as rn
        from {{ bz('t24core_ld_loans_and_deposits_his') }} h
    ) where rn = 1
),
mg as (select * from {{ bz('t24core_mb_mg_saving_multi') }}),
cus as (select * from {{ bz('t24core_customer') }}),
sec as (select * from {{ bz('t24core_sector') }}),
stmt as (
    select * from {{ bz('t24core_stmt_entry') }}
    where {{ t24_date('BOOKING_DATE') }} = {{ etl_date() }} and nvl(COMMIT_ACTION, '_') <> 'D'
)
select
    TRIM(ld.MB_RM_BANCHEO) as LD_MB_RM_BANCHEO,
    {{ t24_date('ld.ORIG_VAL_DATE') }} as LD_ORIG_VAL_DATE,
    COALESCE(CAST(ld.PE_RATE AS DECIMAL(13,7)), 0) as LD_PE_RATE,
    TRIM(ld.PRIN_LIQ_ACCT) as LD_PRIN_LIQ_ACCT,
    TRIM(ld.PRODUCTGR_CODE) as LD_PRODUCTGR_CODE,
    COALESCE(CAST(ld.PS_RATE AS DECIMAL(13,7)), 0) as PS_RATE,
    COALESCE(CAST(ld.REAL_RATE AS DECIMAL(13,7)), 0) as LD_REAL_RATE,
    TRIM(ld.RECID) as LD_RECID,
    TRIM(ld.RECORD_STATUS) as LD_RECORD_STATUS,
    TRIM(ld.STATUS) as STATUS,
    TRIM(ld.USE_OF_LOAN) as LD_USE_OF_LOAN,
    TRIM(ld.USE_OF_LOAN_CHA) as LD_USE_OF_LOAN_CHA,
    {{ t24_date('ld.VALUE_DATE') }} as LD_VALUE_DATE,
    TRIM(ldh.ACC_FCC) as LDH_ACC_FCC,
    COALESCE(CAST(ldh.AMOUNT AS DECIMAL(25,5)), 0) as AMOUNT,
    COALESCE(CAST(ldh.APPROVE_AMOUNT AS DECIMAL(25,5)), 0) as LDH_APPROVE_AMOUNT,
    TRIM(ldh.CATEGORY) as LDH_CATEGORY,
    TRIM(ldh.CO_CODE) as LDH_CO_CODE,
    TRIM(ldh.COMMIT_ACTION) as LDH_COMMIT_ACTION,
    TRIM(ldh.CRA_LD) as CRA_LD,
    COALESCE(CAST(ldh.CURR_NO AS DECIMAL(10)), 0) as LDH_CURR_NO,
    TRIM(ldh.CURRENCY) as LDH_CURRENCY,
    TRIM(ldh.CUSTOMER_ID) as CUSTOMER_ID,
    {{ t24_date('ldh.DATE_TIME') }} as LDH_DATE_TIME,
    COALESCE(CAST(ldh.DRAWDOWN_NET_AMT AS DECIMAL(25,5)), 0) as DRAWDOWN_NET_AMT,
    COALESCE(CAST(ldh.EXCG_RATE AS DECIMAL(13,7)), 0) as EXCG_RATE,
    {{ t24_date('ldh.FIN_MAT_DATE') }} as FIN_MAT_DATE,
    TRIM(ldh.INT_LIQ_ACCT) as INT_LIQ_ACCT,
    TRIM(ldh.INT_RATE_TYPE) as INT_RATE_TYPE,
    {{ t24_date('ldh.INT_VALUE_DATE') }} as INT_VALUE_DATE,
    COALESCE(CAST(ldh.INTEREST_RATE AS DECIMAL(13,7)), 0) as INTEREST_RATE,
    COALESCE(CAST(ldh.INTEREST_SPREAD AS DECIMAL(13,7)), 0) as INTEREST_SPREAD,
    TRIM(ldh.LOC_TERM) as LOC_TERM,
    TRIM(ldh.MB_RM_BANCHEO) as LDH_MB_RM_BANCHEO,
    {{ t24_date('ldh.ORIG_VAL_DATE') }} as LDH_ORIG_VAL_DATE,
    TRIM(ldh.PRIN_LIQ_ACCT) as LDH_PRIN_LIQ_ACCT,
    COALESCE(CAST(ldh.REAL_RATE AS DECIMAL(13,7)), 0) as LDH_REAL_RATE,
    TRIM(ldh.RECID) as LDH_RECID,
    TRIM(ldh.RECORD_STATUS) as LDH_RECORD_STATUS,
    TRIM(ldh.USE_OF_LOAN) as LDH_USE_OF_LOAN,
    TRIM(ldh.USE_OF_LOAN_CHA) as LDH_USE_OF_LOAN_CHA,
    {{ t24_date('ldh.VALUE_DATE') }} as LDH_VALUE_DATE,
    TRIM(sch.CONTRACT_ID) as CONTRACT_ID,
    {{ t24_date('sch.PREV_SCHED_DATE') }} as PREV_SCHED_DATE,
    TRIM(mg.LD_ID) as LD_ID,
    TRIM(mg.PRODUCTGR_CODE) as MG_PRODUCTGR_CODE,
    TRIM(pd.AUTHORISER) as AUTHORISER,
    TRIM(pd.CREDIT_STATUS) as PD_CREDIT_STATUS,
    COALESCE(CAST(pd.CURR_NO AS DECIMAL(10)), 0) as PD_CURR_NO,
    TRIM(pd.CUSTOMER) as PD_CUSTOMER,
    TRIM(pd.FLAG_STATUS) as PD_FLAG_STATUS,
    TRIM(pd.INPUTTER) as INPUTTER,
    TRIM(pd.LOS_FAC_CODE) as LOS_FAC_CODE,
    {{ t24_date('pd.ORIGINAL_MAT') }} as PD_ORIGINAL_MAT,
    TRIM(pd.PD_TYPE) as PD_PD_TYPE,
    COALESCE(CAST(pd.PE_RATE AS DECIMAL(13,7)), 0) as PD_PE_RATE,
    COALESCE(CAST(pd.PENALTY_RATE AS DECIMAL(13,7)), 0) as PD_PENALTY_RATE,
    TRIM(pd.RECID) as PD_RECID,
    COALESCE(CAST(pd.SNQH_CHUYENDOI AS DECIMAL(10)), 0) as SNQH_CHUYENDOI,
    TRIM(pd.USE_OF_LOAN) as PD_USE_OF_LOAN,
    TRIM(pd.USE_OF_LOAN_CHA) as PD_USE_OF_LOAN_CHA,
    COALESCE(CAST(pdh.CURR_NO AS DECIMAL(10)), 0) as PDH_CURR_NO,
    TRIM(pdh.PD_TYPE) as PDH_PD_TYPE,
    TRIM(pdh.RECID) as PDH_RECID,
    TRIM(pdhmv.ACC_FCC) as PDHMV_ACC_FCC,
    COALESCE(CAST(pdhmv.APPROVE_AMOUNT AS DECIMAL(25,5)), 0) as PDHMV_APPROVE_AMOUNT,
    TRIM(pdhmv.CATEGORY) as PDHMV_CATEGORY,
    TRIM(pdhmv.CO_CODE) as PDHMV_CO_CODE,
    TRIM(pdhmv.CREDIT_STATUS) as PDHMV_CREDIT_STATUS,
    COALESCE(CAST(pdhmv.CURR_NO AS DECIMAL(10)), 0) as PDHMV_CURR_NO,
    TRIM(pdhmv.CUSTOMER) as PDHMV_CUSTOMER,
    {{ t24_date('pdhmv.DATE_TIME') }} as PDHMV_DATE_TIME,
    {{ t24_date('pdhmv.FINAL_DUE_DATE') }} as PDHMV_FINAL_DUE_DATE,
    TRIM(pdhmv.FLAG_STATUS) as PDHMV_FLAG_STATUS,
    {{ t24_date('pdhmv.ORIGINAL_MAT') }} as PDHMV_ORIGINAL_MAT,
    TRIM(pdhmv.PAY_TYPE) as PDHMV_PAY_TYPE,
    TRIM(pdhmv.PD_TYPE) as PDHMV_PD_TYPE,
    COALESCE(CAST(pdhmv.PE_RATE AS DECIMAL(13,7)), 0) as PDHMV_PE_RATE,
    COALESCE(CAST(pdhmv.PENALTY_RATE AS DECIMAL(13,7)), 0) as PDHMV_PENALTY_RATE,
    TRIM(pdhmv.RECID) as PDHMV_RECID,
    TRIM(pdhmv.RECORD_STATUS) as PDHMV_RECORD_STATUS,
    TRIM(pdhmv.REPAYMENT_ACCT) as PDHMV_REPAYMENT_ACCT,
    TRIM(pdmv.ACC_FCC) as ACC_FCC,
    COALESCE(CAST(pdmv.APPROVE_AMOUNT AS DECIMAL(25,5)), 0) as APPROVE_AMOUNT,
    TRIM(pdmv.CATEGORY) as CATEGORY,
    TRIM(pdmv.CO_CODE) as CO_CODE,
    TRIM(pdmv.COMMIT_ACTION) as COMMIT_ACTION,
    TRIM(pdmv.CREDIT_STATUS) as CREDIT_STATUS,
    COALESCE(CAST(pdmv.CURR_NO AS DECIMAL(10)), 0) as CURR_NO,
    TRIM(pdmv.CUSTOMER) as CUSTOMER,
    {{ t24_date('pdmv.DATE_TIME') }} as DATE_TIME,
    {{ t24_date('pdmv.FINAL_DUE_DATE') }} as FINAL_DUE_DATE,
    TRIM(pdmv.FLAG_STATUS) as FLAG_STATUS,
    TRIM(pdmv.ID) as ID,
    {{ t24_date('pdmv.ORIGINAL_MAT') }} as ORIGINAL_MAT,
    COALESCE(CAST(pdmv.PAY_AMT_OUTS AS DECIMAL(25,5)), 0) as PAY_AMT_OUTS,
    TRIM(pdmv.PAY_TYPE) as PAY_TYPE,
    {{ t24_date('pdmv.PAYMENT_DTE_DUE') }} as PAYMENT_DTE_DUE,
    TRIM(pdmv.PD_TYPE) as PD_TYPE,
    COALESCE(CAST(pdmv.PE_RATE AS DECIMAL(13,7)), 0) as PE_RATE,
    COALESCE(CAST(pdmv.PENALTY_RATE AS DECIMAL(13,7)), 0) as PENALTY_RATE,
    TRIM(pdmv.RECID) as RECID,
    TRIM(pdmv.RECORD_STATUS) as RECORD_STATUS,
    COALESCE(CAST(pdmv.REPAID_AMT AS DECIMAL(25,5)), 0) as REPAID_AMT,
    TRIM(pdmv.REPAYMENT_ACCT) as REPAYMENT_ACCT,
    TRIM(sec.CUSTOMER_TYPE) as CUSTOMER_TYPE,
    TRIM(sec.RECID) as SEC_RECID,
    COALESCE(CAST(stmt.AMOUNT_FCY AS DECIMAL(25,5)), 0) as AMOUNT_FCY,
    COALESCE(CAST(stmt.AMOUNT_LCY AS DECIMAL(25,5)), 0) as AMOUNT_LCY,
    {{ t24_date('stmt.BOOKING_DATE') }} as BOOKING_DATE,
    TRIM(stmt.CURRENCY) as STMT_CURRENCY,
    TRIM(stmt.OUR_REFERENCE) as OUR_REFERENCE,
    TRIM(stmt.RECID) as STMT_RECID,
    TRIM(stmt.TRANSACTION_CODE) as TRANSACTION_CODE,
    {{ t24_date('stmt.VALUE_DATE') }} as STMT_VALUE_DATE,
    {{ t24_date('pdhmv.PAYMENT_DTE_DUE') }} as PDHMV_PAYMENT_DTE_DUE,
    COALESCE(CAST(pdhmv.PAY_AMT_OUTS AS DECIMAL(25,5)), 0) as PDHMV_PAY_AMT_OUTS,
    {{ etl_date() }} as ETL_DATE
from pdmv
left join pd    on pdmv.RECID = pd.RECID
left join pdhmv on pdmv.RECID = pdhmv.RECID and pdmv.PAY_TYPE = pdhmv.PAY_TYPE
left join pdh   on pdmv.RECID = pdh.RECID
left join sch   on substr(pdmv.RECID, 3) = sch.CONTRACT_ID
left join ld    on substr(pdmv.RECID, 3, 12) = ld.RECID
left join ldh   on substr(pdmv.RECID, 3, 12) = substr(ldh.RECID, 1, instr(ldh.RECID, ';') - 1)
left join mg    on ld.RECID = mg.LD_ID
left join cus   on coalesce(pdmv.CUSTOMER, pd.CUSTOMER, ld.CUSTOMER_ID) = cus.RECID
left join sec   on cus.SECTOR = sec.RECID
left join stmt  on substr(pdmv.RECID, 3) = substr(stmt.OUR_REFERENCE, 3)
