-- Wave 3 | PG_GOLD.DIM_CST (9 cột) – SCD TYPE 2 | Sheet 'Dimension Customer' | Track 7 – Fresher 7
-- CST cst LEFT JOIN IDV_PRFL idv / ORG_PRFL org (cả 3 lọc SYS_EXP_DT IS NULL)
-- Ngành kinh tế cấp 1/2 = NVL(idv.*, org.*)
{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['DIM_KEY'], alias='dim_cst') }}

with src as (
    select
        {{ dim_key("greatest(" ~ etl_change_date('cst') ~ ", coalesce(idv.SYS_UDT_DT, idv.SYS_EFF_DT, org.SYS_UDT_DT, org.SYS_EFF_DT, " ~ etl_change_date('cst') ~ "))", 'cst.CST_ID') }} as DIM_KEY,
        cst.CST_ID,
        cst.CST_CODE,
        cst.CST_NM,
        cst.SCTR_CODE,
        nvl(idv.IDY_LV1_CL_CODE, org.IDY_LV1_CL_CODE) as IDY_LV1_CL_CODE,
        nvl(idv.IDY_LV2_CL_CODE, org.IDY_LV2_CL_CODE) as IDY_LV2_CL_CODE,
        cst.SYS_EFF_DT,
        cast(null as date) as SYS_EXP_DT
    from {{ ref('cst') }} cst
    left join {{ ref('idv_prfl') }} idv on cst.CST_ID = idv.CST_ID and idv.SYS_EXP_DT is null
    left join {{ ref('org_prfl') }} org on cst.CST_ID = org.CST_ID and org.SYS_EXP_DT is null
    where cst.SYS_EXP_DT is null
)
{{ dim_scd2('CST_ID') }}
