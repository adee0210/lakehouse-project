-- Wave 4 (02:45 – chạy cuối cùng) | PG_GOLD.FCT_LOAN_AR (22 cột: 10 Dimension Keys + 12 Measures) – SNAPSHOT
-- Sheet 'Fact Loan Arrangement' | Track 7 – Fresher 7
-- Chống fan-out: AR_BAL khử trùng (AR_ID, CDR_DT, MUD_CODE); subquery e/h/k lọc CDR_DT = ${ETL_DATE};
-- join 8 bảng DIM_* luôn kèm SYS_EXP_DT IS NULL. Số ngày quá hạn dùng MAX (không SUM).
-- Phân vùng theo CDR_DT_DIM_KEY (= ${ETL_DATE}).
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT_DIM_KEY'],
          pre_hook="{{ delete_partition('CDR_DT_DIM_KEY') }}", alias='fct_loan_ar') }}

with b as (
    select distinct AR_ID, CDR_DT, MUD_CODE from {{ ref('ar_bal') }} where CDR_DT = {{ etl_date() }}
),
e as (
    select
        ORIG_AR_ID,
        max(datediff({{ etl_date() }}, e.PNP_PAST_DUE_DT)) + 1 as SO_NGAY_QH_GOC,
        max(datediff({{ etl_date() }}, e.INT_PAST_DUE_DT)) + 1 as SO_NGAY_QH_LAI,
        greatest(nvl(max(datediff({{ etl_date() }}, e.PNP_PAST_DUE_DT)) + 1, 0),
                 nvl(max(datediff({{ etl_date() }}, e.INT_PAST_DUE_DT)) + 1, 0)) as SONGAY_QHAN_CAONHAT,
        max(nvl(e.ADDITION_DYS_IN_ARS, 0)) as SNQH_CHUYENDOI
    from {{ ref('ar_dlq_smy') }} e
    where e.CDR_DT = {{ etl_date() }}
    group by ORIG_AR_ID
),
h as (
    select ORIG_AR_ID, sum(TOT_INT_PAID_AMT_FCY) as TOT_INT_PAID_AMT_FCY,
           sum(TOT_INT_DUE_AMT_FCY) as TOT_INT_DUE_AMT_FCY, sum(TOT_INT_ODUE_AMT_FCY) as TOT_INT_ODUE_AMT_FCY
    from {{ ref('ast_ar_int_smy') }} where CDR_DT = {{ etl_date() }}
    group by ORIG_AR_ID
),
k as (
    select ORIG_AR_ID, sum(CLS_BAL_LCY) as CLS_BAL_LCY
    from {{ ref('ar_tvr_smy') }} where CDR_DT = {{ etl_date() }}
    group by ORIG_AR_ID
)
select
    {{ etl_date() }}                     as CDR_DT_DIM_KEY,
    dim_ar.DIM_KEY                       as LOAN_AR_DIM_KEY,
    dim_cst.DIM_KEY                      as CST_DIM_KEY,
    dim_pd.DIM_KEY                       as PD_DIM_KEY,
    dim_ou.DIM_KEY                       as OU_DIM_KEY,
    dim_ccy.DIM_KEY                      as CCY_DIM_KEY,
    dim_lmt.DIM_KEY                      as LMT_DIM_KEY,
    dim_rstc.DIM_KEY                     as LOAN_RSTC_TP_DIM_KEY,
    dim_fnc_1.DIM_KEY                    as LOAN_AR_FNC_ST_DIM_KEY,
    dim_fnc_2.DIM_KEY                    as CST_FNC_ST_DIM_KEY,
    b.MUD_CODE,
    d.PNY_RATE,
    d.ODUE_INT_RATE,
    cast(nvl(h.TOT_INT_DUE_AMT_FCY, 0) + nvl(h.TOT_INT_ODUE_AMT_FCY, 0) as decimal(32,5)) as TOT_INT_ARS_FCY,
    cast(nvl(h.TOT_INT_PAID_AMT_FCY, 0) as decimal(32,5))                                as TOT_INT_PAID_AMT_FCY,
    cast(nvl(e.SO_NGAY_QH_GOC, 0) as decimal(32,0))                                      as DYS_IN_PNP_ARS,
    cast(nvl(e.SO_NGAY_QH_LAI, 0) as decimal(32,0))                                      as DYS_IN_INT_ARS,
    cast(nvl(e.SONGAY_QHAN_CAONHAT, 0) + nvl(e.SNQH_CHUYENDOI, 0) as decimal(32,0))      as MX_DYS_IN_ARS,
    cast(nvl(e.SNQH_CHUYENDOI, 0) as decimal(32,0))                                      as ADDITION_DYS_IN_ARS,
    cast(nvl(k.CLS_BAL_LCY, 0) as decimal(32,5))                                         as OTSND_AMT_LCY,
    g.MID_RATE                                                                           as EXG_RATE_TO_LCL_CCY,
    c.NBR_OF_DBT_RSTC
from {{ ref('loan_ar') }} a
join b                                    on a.AR_ID = b.AR_ID
left join {{ ref('loan_ar_prfl') }} c     on a.AR_ID = c.AR_ID and c.SYS_EXP_DT is null
left join {{ ref('ar_rate_hist') }} d     on a.AR_ID = d.AR_ID and d.SYS_EXP_DT is null
left join e                               on e.ORIG_AR_ID = a.AR_ID
left join {{ ref('exg_rate') }} g         on a.CCY_CODE = g.FRST_CCY_CODE and g.SNPST_DT = {{ etl_date() }}
left join h                               on a.AR_ID = h.ORIG_AR_ID
left join k                               on a.AR_ID = k.ORIG_AR_ID
left join {{ ref('idv_prfl') }} l         on a.CST_ID = l.CST_ID and l.SYS_EXP_DT is null
left join {{ ref('org_prfl') }} m         on a.CST_ID = m.CST_ID and m.SYS_EXP_DT is null
join {{ ref('dim_loan_ar') }} dim_ar      on a.AR_ID = dim_ar.AR_ID and dim_ar.SYS_EXP_DT is null
join {{ ref('dim_cst') }} dim_cst         on a.CST_ID = dim_cst.CST_ID and dim_cst.SYS_EXP_DT is null
left join {{ ref('dim_pd') }} dim_pd      on a.PD_CODE = dim_pd.PD_CODE and dim_pd.SYS_EXP_DT is null
left join {{ ref('dim_ou') }} dim_ou      on c.OU_CODE = dim_ou.OU_CODE and dim_ou.SYS_EXP_DT is null
left join {{ ref('dim_ccy') }} dim_ccy    on a.CCY_CODE = dim_ccy.CCY_CODE and dim_ccy.SYS_EXP_DT is null
left join {{ ref('dim_lmt') }} dim_lmt    on a.LMT_ID = dim_lmt.LMT_ID and dim_lmt.SYS_EXP_DT is null
left join {{ ref('dim_rstc_tp') }} dim_rstc on c.DBT_RSTC_TP_CODE = dim_rstc.RSTC_TP_CODE and dim_rstc.SYS_EXP_DT is null
left join {{ ref('dim_fnc_st') }} dim_fnc_1 on c.FNC_ST_CODE = dim_fnc_1.FNC_ST_CODE and dim_fnc_1.SYS_EXP_DT is null
left join {{ ref('dim_fnc_st') }} dim_fnc_2 on nvl(l.CST_FNC_ST_CODE, m.CST_FNC_ST_CODE) = dim_fnc_2.FNC_ST_CODE and dim_fnc_2.SYS_EXP_DT is null
where a.SYS_EXP_DT is null
