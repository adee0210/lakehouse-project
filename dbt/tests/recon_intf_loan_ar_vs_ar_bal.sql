-- (c) Đối soát: tổng dư nợ gốc quy đổi INTF_LOAN_AR.AMT_LCY = tổng |CLS_BAL_LCY| dòng gốc (GL 2%, <0) trên PG_SILVER.AR_BAL
with s as (
    select sum(case when GL_ITM_CODE like '2%' and CLS_BAL_LCY < 0 then abs(CLS_BAL_LCY) else 0 end) as v
    from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }}
),
g as (select sum(AMT_LCY) as v from {{ ref('intf_loan_ar') }} where CDR_DT = {{ etl_date() }})
select s.v as silver_ar_bal, g.v as gold_intf_loan_ar from s, g where abs(nvl(s.v, 0) - nvl(g.v, 0)) > 0.01
