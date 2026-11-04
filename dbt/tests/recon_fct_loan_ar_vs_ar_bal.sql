-- (c) Đối soát: tổng FCT_LOAN_AR.OTSND_AMT_LCY = tổng dư nợ gốc quy đổi (GL 2%, <0) trên PG_SILVER.AR_BAL
--     trên tập HĐ mà FCT có thể chứa theo spec (LOAN_AR hiệu lực JOIN DIM_LOAN_AR, DIM_CST – inner join).
--     HĐ gốc của dòng PDLD% = SUBSTR(AR_CODE,3,12). Phần dư nợ của HĐ thiếu KH ở nguồn: xem recon_fct_excluded_balance.
with fct_scope as (
    select l.AR_ID from {{ ref('loan_ar') }} l
    join {{ ref('dim_loan_ar') }} da on l.AR_ID = da.AR_ID and da.SYS_EXP_DT is null
    join {{ ref('dim_cst') }} dc on l.CST_ID = dc.CST_ID and dc.SYS_EXP_DT is null
    where l.SYS_EXP_DT is null
),
ab as (
    select case when AR_CODE like 'PDLD%' then {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'substr(AR_CODE, 3, 12)') }} else AR_ID end as ORIG_AR_ID,
           sum(case when GL_ITM_CODE like '2%' and CLS_BAL_LCY < 0 then abs(CLS_BAL_LCY) else 0 end) as v
    from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }}
    group by 1
),
s as (select sum(v) as v from ab where ORIG_AR_ID in (select AR_ID from fct_scope)
                                   and ORIG_AR_ID in (select AR_ID from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }})),
f as (select sum(OTSND_AMT_LCY) as v from {{ ref('fct_loan_ar') }} where CDR_DT_DIM_KEY = {{ etl_date() }})
select s.v as silver_ar_bal, f.v as gold_fct_loan_ar from s, f where abs(nvl(s.v, 0) - nvl(f.v, 0)) > 0.01
