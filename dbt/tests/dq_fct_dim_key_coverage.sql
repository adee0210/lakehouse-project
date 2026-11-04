-- Cảnh báo khi một DIM key trong FCT_LOAN_AR rỗng 100% (join DIM hỏng hoặc nguồn thiếu dữ liệu).
-- Kết quả hiện tại trên Oracle 2025-10-07: CCY (LD.CURRENCY nguồn NULL 99%) và LMT (bảng LIMIT nguồn chỉ có 2/6 cột).
{{ config(severity='warn') }}
with f as (select * from {{ ref('fct_loan_ar') }} where CDR_DT_DIM_KEY = {{ etl_date() }}),
c as (
    select 'PD_DIM_KEY' as dim_key, count(PD_DIM_KEY) as filled, count(*) as total from f
    union all select 'OU_DIM_KEY', count(OU_DIM_KEY), count(*) from f
    union all select 'CCY_DIM_KEY', count(CCY_DIM_KEY), count(*) from f
    union all select 'LMT_DIM_KEY', count(LMT_DIM_KEY), count(*) from f
    union all select 'LOAN_AR_FNC_ST_DIM_KEY', count(LOAN_AR_FNC_ST_DIM_KEY), count(*) from f
    union all select 'CST_FNC_ST_DIM_KEY', count(CST_FNC_ST_DIM_KEY), count(*) from f
)
select * from c where total > 0 and filled = 0
