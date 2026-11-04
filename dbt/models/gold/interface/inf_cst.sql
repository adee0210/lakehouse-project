-- Wave 4 (02:15) | PG_GOLD.INF_CST (64 cột) – SNAPSHOT (CDR_DT) | Sheet 'Interface Customer' | Track 1 – Fresher 1
-- Nhánh 1 (KHCN): CST a JOIN IDV_PRFL e (+ IDV_EMP_ST f) | Nhánh 2 (KHDN): CST a JOIN ORG_PRFL e
-- Mọi bảng SCD lọc SYS_EXP_DT IS NULL; IP_ALT_IDENTN khử trùng RN = 1 / CST_CODE (IDENTN_ISSU_DT DESC);
-- c1/c2/c3 = EMAIL / HOME_NUMBER / MOBILE_NUMBER, d1/d2 = CURRENT / PERMANENT ADDRESS.
{{ config(materialized='incremental', incremental_strategy='append', partition_by=['CDR_DT'],
          pre_hook="{{ delete_partition('CDR_DT') }}", alias='inf_cst') }}

{#- (cột đích, biểu thức nhánh KHCN, biểu thức nhánh KHDN) – đúng thứ tự 64 cột của sheet -#}
{%- set cols = [
    ('CST_CODE', 'a.CST_CODE', 'a.CST_CODE'),
    ('OPN_DT', 'a.OPN_DT', 'a.OPN_DT'),
    ('FRST_OU_CODE', 'a.FRST_OU_CODE', 'a.FRST_OU_CODE'),
    ('IDENTN_NBR', 'b.IDENTN_NBR', 'b.IDENTN_NBR'),
    ('IDENTN_ISSU_DT', 'b.IDENTN_ISSU_DT', 'b.IDENTN_ISSU_DT'),
    ('BSN_RGST_NBR', 'cast(null as string)', 'e.BSN_RGST_NBR'),
    ('TAX_IDENTN_NBR', 'e.TAX_IDENTN_NBR', 'e.TAX_IDENTN_NBR'),
    ('BSN_RGST_EFF_DT', 'e.BSN_RGST_EFF_DT', 'e.BSN_RGST_EFF_DT'),
    ('BSN_RGST_EXPIRY_DT', 'b.IDENTN_EXPIRY_DT', 'b.IDENTN_EXPIRY_DT'),
    ('GND', 'e.GND', 'cast(null as string)'),
    ('CHKER_OFCR', 'a.CHKER_OFCR_CODE', 'a.CHKER_OFCR_CODE'),
    ('DOB', 'e.DOB', 'e.ESTB_DT'),
    ('OU_CODE', 'a.OU_CODE', 'a.OU_CODE'),
    ('VIP_TP_CODE', 'e.VIP_TP_CODE', 'e.VIP_TP_CODE'),
    ('VIP_EFF_DT', 'e.VIP_EFF_DT', 'e.VIP_EFF_DT'),
    ('EMPE_CODE', 'a.EMPE_CODE', 'a.EMPE_CODE'),
    ('SHRT_NM', 'a.SHRT_NM', 'a.SHRT_NM'),
    ('CST_NM', 'a.CST_NM', 'a.CST_NM'),
    ('CDR_DT', etl_date(), etl_date()),
    ('SCTR_CODE', 'a.SCTR_CODE', 'a.SCTR_CODE'),
    ('CST_GRP_CODE', 'a.CST_GRP_CODE', 'a.CST_GRP_CODE'),
    ('MAKER_OFCR', 'a.MAKER_OFCR_CODE', 'a.MAKER_OFCR_CODE'),
    ('CTY_NM', 'd1.CTY_NM', 'd1.CTY_NM'),
    ('CST_ST_CODE', 'e.CST_ST_CODE', 'e.CST_ST_CODE'),
    ('CTY_NM_2', 'd2.CTY_NM', 'd2.CTY_NM'),
    ('CTY_OF_CTZN_CODE', 'e.CTY_OF_CTZN_CODE', 'cast(null as string)'),
    ('OCP_CODE', 'f.OCP_CODE', 'cast(null as string)'),
    ('EMAIL_ADRESS', 'c1.ELC_ADR_VAL', 'c1.ELC_ADR_VAL'),
    ('VIP_F', 'e.VIP_F', 'e.VIP_F'),
    ('KYC_CNL_CODE', 'e.KYC_CNL_CODE', 'e.KYC_CNL_CODE'),
    ('CST_LCS_CODE', 'e.CST_LCS_CODE', 'e.CST_LCS_CODE'),
    ('STR_NM', 'd1.STR_NM', 'd1.STR_NM'),
    ('STR_ADDR', 'a.STR_ADDR', 'a.STR_ADDR'),
    ('TRGT_CODE', 'a.TRGT_CODE', 'a.TRGT_CODE'),
    ('IDENTN_TP_CODE', 'b.IDENTN_TP_CODE', 'b.IDENTN_TP_CODE'),
    ('GEN_DIR_PLC', 'cast(null as string)', 'e.GEN_DIR_PLC'),
    ('OPN_OU_CODE', 'a.FRST_OU_CODE', 'a.FRST_OU_CODE'),
    ('MGT_OU_CODE', 'e.OU_CODE', 'e.OU_CODE'),
    ('DIR_DOB', 'cast(null as date)', 'e.DIR_DOB'),
    ('NAT_ID_NBR', 'b.IDENTN_NBR', 'b.IDENTN_NBR'),
    ('SCD_OFCR_CODE', 'a.CHKER_OFCR_CODE', 'a.CHKER_OFCR_CODE'),
    ('ISSU_PLC_CODE', 'b.ISSU_PLC_CODE', 'b.ISSU_PLC_CODE'),
    ('CNTCT_PRSN', 'cast(null as string)', 'e.CNTCT_PRSN'),
    ('MBL_BNKG_F', 'e.MBL_BNKG_F', 'e.MBL_BNKG_F'),
    ('INN_BNKG_F', 'e.INN_BNKG_F', 'e.INN_BNKG_F'),
    ('AML_RSLT', 'e.AML_RSLT', 'e.AML_RSLT'),
    ('AML_CHK_F', 'e.AML_CHK_F', 'e.AML_CHK_F'),
    ('IDV_EMP_ST_CODE', 'f.IDV_EMP_ST_CODE', 'cast(null as string)'),
    ('JNT_LBY_F', 'e.JNT_LBY_F', 'e.JNT_LBY_F'),
    ('MB_CR_ST', 'e.MB_CR_ST', 'e.MB_CR_ST'),
    ('CST_FNC_ST_CODE', 'e.CST_FNC_ST_CODE', 'e.CST_FNC_ST_CODE'),
    ('ANUL_INCM', 'f.ANUL_INCM', 'cast(null as decimal(25,5))'),
    ('SRC_DT_TM', 'a.SRC_DT_TM', 'a.SRC_DT_TM'),
    ('PROV_NM', 'd1.PROV_NM', 'd1.PROV_NM'),
    ('IDY_LV1_CL_CODE', 'e.IDY_LV1_CL_CODE', 'e.IDY_LV1_CL_CODE'),
    ('REFR_CODE', 'a.REFR_CODE', 'a.REFR_CODE'),
    ('CNTCT_PRSN_PHN', 'cast(null as string)', 'e.CNTCT_PRSN_PHN'),
    ('CTR_TMPL_CODE', 'a.CTR_TMPL_CODE', 'a.CTR_TMPL_CODE'),
    ('CITY_NM', 'd2.PROV_NM', 'd2.PROV_NM'),
    ('VSS_CODE', 'e.VSS_CODE', 'cast(null as string)'),
    ('CRS_SELL_RM_CODE', 'a.CRS_SELL_RM_CODE', 'a.CRS_SELL_RM_CODE'),
    ('HM_NBR', 'c2.ELC_ADR_VAL', 'c2.ELC_ADR_VAL'),
    ('MBL_NBR', 'c3.ELC_ADR_VAL', 'c3.ELC_ADR_VAL'),
    ('IDY_LV2_CL_CODE', 'e.IDY_LV2_CL_CODE', 'e.IDY_LV2_CL_CODE'),
] -%}

with a as (select * from {{ ref('cst') }} where SYS_EXP_DT is null),
b as (
    select * from (
        select x.*, row_number() over (partition by x.CST_CODE order by x.IDENTN_ISSU_DT desc) as RN
        from {{ ref('ip_alt_identn') }} x where x.SYS_EXP_DT is null
    ) where RN = 1
),
elc as (select * from {{ ref('ip_elc_adr') }} where SYS_EXP_DT is null),
pst as (select * from {{ ref('ip_pst_adr') }} where SYS_EXP_DT is null)
-- Nhánh 1: Khách hàng cá nhân
select
{%- for c in cols %}
    {{ c[1] }} as {{ c[0] }}{{ "," if not loop.last }}
{%- endfor %}
from a
join {{ ref('idv_prfl') }} e on a.CST_ID = e.CST_ID and e.SYS_EXP_DT is null
left join {{ ref('idv_emp_st') }} f on a.CST_ID = f.CST_ID and f.SYS_EXP_DT is null
left join b  on a.CST_ID = b.CST_ID
left join elc c1 on a.CST_ID = c1.CST_ID and c1.ELC_ADR_TP = 'EMAIL'
left join elc c2 on a.CST_ID = c2.CST_ID and c2.ELC_ADR_TP = 'HOME_NUMBER'
left join elc c3 on a.CST_ID = c3.CST_ID and c3.ELC_ADR_TP = 'MOBILE_NUMBER'
left join pst d1 on a.CST_ID = d1.CST_ID and d1.PST_ADR_TP = 'CURRENT ADDRESS'
left join pst d2 on a.CST_ID = d2.CST_ID and d2.PST_ADR_TP = 'PERMANENT ADDRESS'

union all

-- Nhánh 2: Khách hàng doanh nghiệp / tổ chức
select
{%- for c in cols %}
    {{ c[2] }} as {{ c[0] }}{{ "," if not loop.last }}
{%- endfor %}
from a
join {{ ref('org_prfl') }} e on a.CST_ID = e.CST_ID and e.SYS_EXP_DT is null
left join b  on a.CST_ID = b.CST_ID
left join elc c1 on a.CST_ID = c1.CST_ID and c1.ELC_ADR_TP = 'EMAIL'
left join elc c2 on a.CST_ID = c2.CST_ID and c2.ELC_ADR_TP = 'HOME_NUMBER'
left join elc c3 on a.CST_ID = c3.CST_ID and c3.ELC_ADR_TP = 'MOBILE_NUMBER'
left join pst d1 on a.CST_ID = d1.CST_ID and d1.PST_ADR_TP = 'CURRENT ADDRESS'
left join pst d2 on a.CST_ID = d2.CST_ID and d2.PST_ADR_TP = 'PERMANENT ADDRESS'
