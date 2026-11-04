-- Wave 4 (02:30) | PG_GOLD.INTF_LOAN_REPYMT_SHD (20 cột) – SNAPSHOT (CDR_DT) | Sheet 'Interface Loan Repayment Schedu' | Track 4
-- arr_shd: AR_AVY_SHD_TP = 'LOAN_PD_SHD' AND ${ETL_DATE} <= EFF_MAT_DT AND SYS_EXP_DT IS NULL
-- + LOAN_AR, LOAN_AR_PRFL, CST, IDV_PRFL, ORG_PRFL (SYS_EXP_DT IS NULL)
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='intf_loan_repymt_shd') }}

select
    {{ etl_date() }}                     as CDR_DT,
    arr_shd.CST_ID,
    arr_shd.CST_CODE,
    arr_shd.AR_AVY_SHD_CODE,
    arr_shd.AR_CODE,
    arr_prfl.OU_CODE,
    arr_shd.EFF_MAT_DT,
    arr_shd.CCY_CODE,
    arr_shd.PBL_AMT_TP,
    arr_shd.PBL_AMT_LCY,
    arr_shd.PBL_AMT_FCY,
    arr_shd.EXG_RATE,
    'DUE'                                as LND_REPYMT_TP,
    arr_shd.F_LAST_OD_DT,
    arr_shd.LAST_OD_DT,
    arr_loan.FCC_VAL_DT,
    arr_loan.MAT_DT,
    arr_prfl.GL_ITM_CODE,
    cast(coalesce(cst_org.CST_FNC_ST_CODE, cst_idv.CST_FNC_ST_CODE, '1') as decimal(1,0)) as CIC_LOAN_GRP,
    cst.CST_NM                           as CST_FULL_NM
from {{ ref('ar_avy_shd_itm') }} arr_shd
left join {{ ref('loan_ar') }} arr_loan      on arr_shd.AR_ID = arr_loan.AR_ID and arr_loan.SYS_EXP_DT is null
left join {{ ref('loan_ar_prfl') }} arr_prfl on arr_shd.AR_ID = arr_prfl.AR_ID and arr_prfl.SYS_EXP_DT is null
left join {{ ref('cst') }} cst               on arr_shd.CST_ID = cst.CST_ID and cst.SYS_EXP_DT is null
left join {{ ref('idv_prfl') }} cst_idv      on arr_shd.CST_ID = cst_idv.CST_ID and cst_idv.SYS_EXP_DT is null
left join {{ ref('org_prfl') }} cst_org      on arr_shd.CST_ID = cst_org.CST_ID and cst_org.SYS_EXP_DT is null
where arr_shd.AR_AVY_SHD_TP = 'LOAN_PD_SHD'
  and {{ etl_date() }} <= arr_shd.EFF_MAT_DT
  and arr_shd.SYS_EXP_DT is null
