-- Wave 1 | PG_SILVER.CV (8 cột) – SCD TYPE 1 (LOOKUP MASTER) | Sheet 'Classification Value' | Track 3 – Fresher 3
-- 2 bộ mã: LOAN_RSTC_TP (loại cơ cấu nợ 1..7) và FNC_ST (nhóm nợ 1..5).
-- Seed 12 mã chuẩn (dòng 26-38 của sheet) UNION DISTINCT với mã phát sinh thực tế -> không trùng CL_ID.
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['CL_ID'], alias='cv') }}

with rstc as (
    select code from values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7') as t(code)
    union
    select distinct cast(ud.CO_CAU_NO as string) from {{ bz('flexbo_pgbld_rt_contract_udfield_hist') }} ud
    where ud.CO_CAU_NO is not null
),
fnc as (
    select code from values ('1'), ('2'), ('3'), ('4'), ('5') as t(code)
    union
    select distinct cast(cust.ASSET_CLASS as string) from {{ bz('t24core_customer') }} cust
    where cust.ASSET_CLASS is not null
),
src as (
    select
        {{ fn_hash('T24_CL_LOAN_RSTC_TP', 'ud.code') }} as CL_ID,
        ud.code                                         as CL_CODE,
        decode(ud.code, '1', '1.Co cau theo thong tu 09', '2', '2.Co cau theo quyet dinh 780',
                        '3', '3.Co cau khac (theo thong tu 02 hien hanh)', '4', '4.Khong',
                        '5', '5.Co cau no do Covid-19', '6', '6.Mien giam lai do Covid-19',
                        '7', '7.Co cau va mien giam lai do Covid-19') as CL_DSC,
        'LOAN_RSTC_TP'                                  as CL_SCM_CODE,
        'T24_CL_LOAN_RSTC_TP'                           as SRC_STM_CODE
    from rstc ud
    union all
    select
        {{ fn_hash('T24_CL_FNC_ST', 'f.code') }},
        f.code,
        concat('Nhom no ', f.code),
        'FNC_ST',
        'T24_CL_FNC_ST'
    from fnc f
)
{{ scd1(['CL_ID']) }}
