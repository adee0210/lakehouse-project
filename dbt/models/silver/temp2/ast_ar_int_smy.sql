-- Wave 2 | PG_SILVER.AST_AR_INT_SMY (10 cột) – SNAPSHOT (CDR_DT) | Sheet 'Asset Arrangement Interest Summ' | Track 6
-- Lãi dự thu (394% + 94%), lãi đã trả (STMT_ENTRY lũy kế <= ${ETL_DATE}), lãi trong hạn (394%), lãi quá hạn ngoại bảng (94%).
-- SUM có lọc SBV_CODE để không cộng nhầm dư nợ gốc (2%); b gom trước theo hợp đồng để không nhân bản TOT_INT_PAID.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='ast_ar_int_smy') }}

with b as (
    select
        sum(case when b.OUR_REFERENCE like 'PDPD%' and b.TRANSACTION_CODE in ('751', '434')
                 then decode(b.CURRENCY, 'VND', abs(cast(b.AMOUNT_LCY as decimal(25,5))), abs(cast(b.AMOUNT_FCY as decimal(25,5)))) else 0 end)
      + sum(case when (b.OUR_REFERENCE like 'PDLD%' or b.OUR_REFERENCE like 'LD%') and b.CRF_PROD_CAT = '21069'
                      and b.TRANSACTION_CODE in ('750', '420')
                 then decode(b.CURRENCY, 'VND', abs(cast(b.AMOUNT_LCY as decimal(25,5))), abs(cast(b.AMOUNT_FCY as decimal(25,5)))) else 0 end) as SUM_REPAID_AMT,
        case when b.OUR_REFERENCE like 'LD%' or b.OUR_REFERENCE like 'PD%' then b.OUR_REFERENCE end as RECID
    from {{ bz('t24core_stmt_entry') }} b
    where b.RECID not like 'F%' and nvl(b.COMMIT_ACTION, '_') <> 'D' and {{ t24_date('b.BOOKING_DATE') }} <= {{ etl_date() }}
    group by case when b.OUR_REFERENCE like 'LD%' or b.OUR_REFERENCE like 'PD%' then b.OUR_REFERENCE end
),
a as (
    -- ép DECIMAL tường minh cho số tiền VARCHAR2
    select APP_ID, SBV_CODE, TXN_DATE, APP_CODE, cast(AMOUNT_CUR as decimal(25,5)) as AMOUNT_CUR
    from {{ bz('source_saoke_crb') }}
    where APP_CODE in ('LD', 'PD') and {{ t24_date('TXN_DATE') }} = {{ etl_date() }}
)
select
    {{ etl_date() }}                                                        as CDR_DT,
    'SAOKE_CRB'                                                             as SRC_STM_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', 'a.APP_ID') }}                  as AR_ID,
    cast(a.APP_ID as string)                                                as AR_CODE,
    {{ fn_hash('T24_LD_LOANS_AND_DEPOSITS', "case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end") }} as ORIG_AR_ID,
    case when a.APP_ID like 'PDLD%' then substr(a.APP_ID, 3, 12) else a.APP_ID end as ORIG_AR_CODE,
    cast(sum(case when a.SBV_CODE like '394%' or a.SBV_CODE like '94%' then abs(nvl(a.AMOUNT_CUR, 0)) else 0 end) as decimal(32,5)) as TOT_ACR_INT_AMT_FCY,
    cast(nvl(b.SUM_REPAID_AMT, 0) as decimal(32,5))                         as TOT_INT_PAID_AMT_FCY,
    cast(sum(case when a.SBV_CODE like '394%' then abs(nvl(a.AMOUNT_CUR, 0)) else 0 end) as decimal(32,5)) as TOT_INT_DUE_AMT_FCY,
    cast(sum(case when a.SBV_CODE like '94%' then abs(nvl(a.AMOUNT_CUR, 0)) else 0 end) as decimal(32,5))  as TOT_INT_ODUE_AMT_FCY
from a
left join b on a.APP_ID = b.RECID
group by a.TXN_DATE, a.APP_ID, b.SUM_REPAID_AMT
