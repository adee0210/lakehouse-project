-- CẢNH BÁO độ đầy đủ nguồn: dư nợ gốc trên AR_BAL của HĐ KHÔNG vào được FCT_LOAN_AR vì thiếu KH (DIM_CST)
-- hoặc thiếu HĐ (LOAN_AR) ở nguồn. Không phải lỗi logic ETL -> severity warn.
{{ config(severity='warn') }}
with ab as (
    select case when AR_CODE like 'PDLD%' then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(AR_CODE, 3, 12)') }} else AR_ID end as ORIG_AR_ID,
           sum(case when GL_ITM_CODE like '2%' and CLS_BAL_LCY < 0 then abs(CLS_BAL_LCY) else 0 end) as v
    from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }} group by 1
),
scope as (
    select l.AR_ID from {{ ref('loan_ar') }} l
    join {{ ref('dim_cst') }} dc on l.CST_ID = dc.CST_ID and dc.SYS_EXP_DT is null
    where l.SYS_EXP_DT is null
)
select count(*) as excluded_contracts, sum(v) as excluded_balance_lcy
from ab where ORIG_AR_ID not in (select AR_ID from scope) and v > 0
having count(*) > 0
