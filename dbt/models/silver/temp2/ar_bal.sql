-- Wave 2 | PG_SILVER.AR_BAL (16 cột) – SNAPSHOT (CDR_DT) | Sheet 'Arrangement Balance' | Track 6 – Fresher 6
-- Số dư hợp đồng theo từng mã GL: WHERE APP_CODE IN ('LD','PD') AND TXN_DATE = ${ETL_DATE}
-- Grain: (CDR_DT, AR_ID, GL_ITM_CODE) – 1 HĐ có nhiều dòng (2% gốc, 394% lãi dự thu, 94% lãi ngoại bảng).
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='ar_bal') }}

select
    cast(a.AMOUNT_CUR as decimal(25,5))                  as RCVB_AMT_LCY,
    cast(a.AMOUNT_LCY as decimal(25,5))                  as CLS_BAL_LCY,
    cast(a.APP_CODE as string)                           as MUD_CODE,
    cast(a.APP_ID as string)                             as AR_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.APP_ID') }} as AR_ID,
    cast(a.ASSET_TYPE as string)                         as AST_TP_CODE,
    cast(a.CATEG_CODE as string)                         as PD_CGY_CODE,
    cast(a.CO_CODE as string)                            as OU_CODE,
    cast(a.CURR as string)                               as CCY_CODE,
    cast(a.CUS_ID as string)                             as CST_CODE,
    {{ fn_hash('T24_CUSTOMER', 'a.CUS_ID') }}            as CST_ID,
    cast(a.LOC_TERM as string)                           as AR_TERM_TP_CODE,
    cast(a.SBV_CODE as string)                           as GL_ITM_CODE,
    {{ fn_hash('T24_GL_ITM', 'a.SBV_CODE') }}            as GL_ITM_ID,
    cast(a.SUB_PRO as string)                            as SUB_PD_CODE,
    {{ t24_date('a.TXN_DATE') }}                         as CDR_DT
from {{ bz('source_saoke_crb') }} a
where a.APP_CODE in ('LD', 'PD') and {{ t24_date('a.TXN_DATE') }} = {{ etl_date() }}
