-- Wave 1 | PG_SILVER.EXG_RATE (5 cột) – SNAPSHOT (SNPST_DT = ${ETL_DATE}) | Sheet 'Exchange Rate' | Track 5 – Fresher 5
-- Tỷ giá MID mới nhất tính đến ${ETL_DATE}: hợp nhất CURRENCY (curr_no = 999999, ưu tiên) và CURRENCY_HIS.
-- Ép DECIMAL(13,7) (không phải int) để giữ phần thập phân.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['SNPST_DT'],
          pre_hook="{{ delete_partition('SNPST_DT') }}", alias='exg_rate') }}

with cur as (
    select
        cur.ID as FRST_CCY_CODE,
        'VND' as SCD_CCY_CODE,
        coalesce(cast(regexp_extract(cast(cur.MID_REVAL_RATE as string), '^([^#]+)', 1) as decimal(13,7)), 1) as mid_reval_rate,
        cur.DATE_TIME as date_time_value,
        999999 as curr_no,
        rank() over (partition by cur.ID order by cur.DATE_TIME desc) as rn
    from {{ bz('t24core_currency') }} cur
    where {{ t24_date('cur.DATE_TIME') }} < date_add({{ etl_date() }}, 1)
),
cur_his as (
    select
        regexp_extract(ch.ID, '^([^;]+)', 1) as FRST_CCY_CODE,
        'VND' as SCD_CCY_CODE,
        coalesce(cast(ch.MID_REVAL_RATE as decimal(13,7)), 1) as mid_reval_rate,
        ch.DATE_TIME as date_time_value,
        cast(ch.CURR_NO as int) as curr_no,
        rank() over (partition by regexp_extract(ch.ID, '^([^;]+)', 1) order by ch.DATE_TIME desc) as rn
    from {{ bz('t24core_currency_his') }} ch
    where {{ t24_date('ch.DATE_TIME') }} < date_add({{ etl_date() }}, 1)
),
cur_main as (
    select FRST_CCY_CODE, SCD_CCY_CODE, mid_reval_rate, date_time_value
    from (
        select u.*, row_number() over (partition by FRST_CCY_CODE order by curr_no desc) as rnk
        from (
            select * from cur where rn = 1
            union all
            select * from cur_his where rn = 1
        ) u
    ) where rnk = 1
)
select
    {{ etl_date() }}                           as SNPST_DT,
    'T24_CURRENCY'                             as SRC_STM_CODE,
    cast(FRST_CCY_CODE as string)              as FRST_CCY_CODE,
    SCD_CCY_CODE                               as SCD_CCY_CODE,
    cast(mid_reval_rate as decimal(25,7))      as MID_RATE
from cur_main
