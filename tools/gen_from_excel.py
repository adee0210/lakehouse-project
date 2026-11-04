"""
Sinh code dbt từ tài liệu BA (docs/Training_Fresher_DeepChecked.xlsx).

Đầu ra:
  scripts/source_schema.json                 – 28 bảng nguồn / 392 cột (kiểu Spark) cho mock + Oracle
  dbt/models/bronze/*.sql + _bronze.yml       – 28 job Wave 0 (1:1 + 3 cột audit) + COMMENT cột
  dbt/models/**/_docs_<table>.yml             – COMMENT cột cho các bảng Silver/Gold (lấy từ sheet mapping)
  dbt/models/silver/temp*/<staging>.sql       – 4 bảng staging nhiều cột (sk_lich_tra_no, CUST_INFO_DAILY,
                                                Saoke_GIAINGAN, SAOKE_TINDUNG) sinh từ cột Transform
  airflow/dags/jobs.json                      – 69 job (GRP_OBJ, WAITING, SCHEDULE, SQL_EXC, PIC) từ Job_dependence

Chạy:  python tools/gen_from_excel.py
"""
import json
import os
import re
from collections import OrderedDict

import openpyxl

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX = os.path.join(ROOT, "docs", "Training_Fresher_DeepChecked.xlsx")
wb = openpyxl.load_workbook(XLSX, data_only=True)


def rows(sheet):
    return [[("" if c is None else str(c).strip()) for c in r] for r in wb[sheet].iter_rows(values_only=True)]


def spark_type(t, length):
    t = (t or "").upper()
    length = (length or "").strip()
    if t in ("VARCHAR2", "CHAR", "VARCHAR"):
        return "STRING"
    if t == "DATE":
        return "DATE"
    if t == "TIMESTAMP":
        return "TIMESTAMP"
    if t == "NUMBER":
        if "." in length:
            p, s = length.split(".")
            return f"DECIMAL({int(p)},{int(s)})"
        if length:
            return f"DECIMAL({int(float(length))},0)"
        return "DECIMAL(38,10)"
    return "STRING"


def clean(s):
    return (s or "").replace("'", "").replace('"', "").replace("\n", " ").replace("⏎", " ").strip()


def write(path, content):
    path = os.path.join(ROOT, path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)


# --------------------------------------------------------------------------- Bronze
bm = rows("Bronze_Mapping_1_1")
bronze_tables = OrderedDict()
for r in [x for x in bm if x[0].isdigit() and x[5]][:28]:
    no, src_schema, src_table, _short, _tgt_schema, tgt, *_ = r
    bronze_tables[tgt] = {"src_schema": src_schema, "src_table": src_table, "columns": []}
for r in bm[31:]:
    if not r[7] or r[0] == "Nguồn":
        continue
    bronze_tables[r[7]]["columns"].append(
        {"name": r[8], "type": spark_type(r[9], r[10]), "ora_type": r[9], "length": r[10], "comment": clean(r[11])}
    )
assert len(bronze_tables) == 28, len(bronze_tables)
assert sum(len(t["columns"]) for t in bronze_tables.values()) == 392

write("scripts/source_schema.json", json.dumps(bronze_tables, ensure_ascii=False, indent=2))

src_groups = OrderedDict()
for tgt, t in bronze_tables.items():
    src_groups.setdefault(t["src_schema"], []).append((tgt, t))

yml = ["version: 2", "", "sources:"]
for schema, items in src_groups.items():
    yml += [
        f"  - name: {schema.lower()}",
        f"    description: Hệ thống nguồn {schema} (Oracle) – mock/oracle tuỳ var source_mode",
        f"    schema: \"{{{{ 'spark_catalog.ora_{schema.lower()}' if var('source_mode', 'mock') == 'oracle' else 'src_{schema.lower()}' }}}}\"",
        "    tables:",
    ]
    for tgt, t in items:
        tbl = t["src_table"].split("@")[0].lower()
        yml += [f"      - name: {tbl}", f"        description: \"{schema}.{t['src_table']}\""]
yml += ["", "models:"]

for tgt, t in bronze_tables.items():
    model = tgt.lower()
    src_name = t["src_schema"].lower()
    src_tbl = t["src_table"].split("@")[0].lower()
    sql = f"""-- Wave 0 | {t['src_schema']}.{t['src_table']} -> PG_BRONZE.{tgt}
-- 1:1 Raw Mirror ({len(t['columns'])} cột nguồn) + 3 cột Audit: ETL_DATE, INGEST_TMS, SRC_STM_CODE
-- Job_dependence: sql/bronze/{model}.sql | schedule 30 0 * * *
{{{{ config(alias='{model}') }}}}

select
    src.*,
    {{{{ etl_date() }}}} as ETL_DATE,
    current_timestamp() as INGEST_TMS,
    '{tgt}' as SRC_STM_CODE
from {{{{ source('{src_name}', '{src_tbl}') }}}} src
"""
    write(f"dbt/models/bronze/{model}.sql", sql)
    yml += [
        f"  - name: {model}",
        f"    description: \"Bronze 1:1 từ {t['src_schema']}.{t['src_table']} ({len(t['columns'])} cột + 3 audit)\"",
        "    columns:",
    ]
    for c in t["columns"]:
        yml += [f"      - name: {c['name'].lower()}", f"        description: \"{c['comment'] or c['name']} ({c['ora_type']} {c['length']})\""]
    yml += [
        "      - name: etl_date",
        "        description: \"Ngày dữ liệu ETL (audit)\"",
        "        tests: [not_null]",
        "      - name: ingest_tms",
        "        description: \"Thời điểm ingest (audit)\"",
        "      - name: src_stm_code",
        "        description: \"Mã bảng nguồn Bronze (audit)\"",
    ]
write("dbt/models/bronze/_bronze.yml", "\n".join(yml) + "\n")

# --------------------------------------------------------------------------- Column docs Silver/Gold
DOC_SHEETS = {
    # sheet: (layer folder, table, header-col-index of target col, type col, length col, desc col)
    "Customer": "silver/temp2", "Individual Profile": "silver/temp2", "Individual Employment Status": "silver/temp2",
    "Organization Profile": "silver/temp2", "Involved Party Alternative Iden": "silver/temp1",
    "Involved Party Electronic Addre": "silver/temp1", "Involved Party Postal Address": "silver/temp1",
    "Loan Arrangement": "silver/temp2", "Loan Arrangement Profile": "silver/temp2",
    "Arrangement x Involved Party": "silver/temp2", "Currency": "silver/temp1", "Product": "silver/temp1",
    "Organization Unit": "silver/temp1", "Employee": "silver/temp1", "Limit": "silver/temp1",
    "Classification Value": "silver/temp1", "Arrangement Activity Schedule I": "silver/temp2",
    "sk_lich_tra_no": "silver/temp1", "Saoke_GIAINGAN": "silver/temp2", "Exchange Rate": "silver/temp1",
    "Posting Entry": "silver/temp2", "SAOKE_TINDUNG": "silver/temp2", "CUST_INFO_DAILY": "silver/temp1",
    "Arrangement Balance": "silver/temp2", "Arrangement Rate History": "silver/temp1",
    "Arrangement Delinquency Summary": "silver/temp2", "Arrangement Turnover Summary": "silver/temp2",
    "Asset Arrangement Interest Summ": "silver/temp2",
    "Dimension Loan Arrangement": "gold/dim", "Dimension Customer": "gold/dim", "Dimension Product": "gold/dim",
    "Dimension Organization Unit": "gold/dim", "Dimension Currency": "gold/dim", "Dimension Limit": "gold/dim",
    "Dimension Loan Restructure Type": "gold/dim", "Dimension Financial Status": "gold/dim",
    "Interface Customer": "gold/interface", "Interface Loan Arrangement": "gold/interface",
    "Interface Loan Disbursement": "gold/interface", "Interface Loan Repayment Schedu": "gold/interface",
    "Fact Loan Arrangement": "gold/fact",
}


def mapping_rows(sheet):
    """Trả về list các dòng mapping (dict) theo header 'Nguồn | Bảng nguồn | Cột nguồn | Transform ...'."""
    out, header, branch = [], None, 1
    for r in rows(sheet):
        if r and r[0].upper().startswith("UNION ALL"):
            branch += 1
            header = None
            continue
        if r and r[0] == "Nguồn" and len(r) > 3 and r[3] == "Transform":
            header = r
            continue
        if header and r and r[0] in ("PG_BRONZE", "PG_SILVER", "PG_GOLD", "PG_SILVER, PG_GOLD", "PG_BRONZE, PG_SILVER"):
            d = {h: (r[i] if i < len(r) else "") for i, h in enumerate(header) if h}
            d["_branch"] = branch
            out.append(d)
        elif header and r and r[0] in ("No", "CL_ID"):
            header = None
    return out


docs_index = {}
for sheet, folder in DOC_SHEETS.items():
    mr = mapping_rows(sheet)
    first = [m for m in mr if m["_branch"] == 1]
    tbl = first[0]["Bảng đích"]
    cols = OrderedDict()
    for m in first:
        col = m["Cột đích"].split("\n")[-1].strip()
        if col and col not in cols:
            cols[col] = {
                "type": spark_type(m.get("Kiểu dữ liệu"), m.get("Độ dài")),
                "desc": clean(m.get("Diễn giải") or m.get("Attribute") or col),
                "transform": m.get("Transform", ""),
            }
    docs_index[tbl.upper()] = {"sheet": sheet, "folder": folder, "columns": cols}

# --------------------------------------------------------------------------- Staging (Silver) 4 bảng lớn
STAGING = {
    "sk_lich_tra_no": ("silver/temp1", "sk_lich_tra_no.sql.j2"),
    "CUST_INFO_DAILY": ("silver/temp1", "cust_info_daily.sql.j2"),
    "Saoke_GIAINGAN": ("silver/temp2", "saoke_giaingan.sql.j2"),
    "SAOKE_TINDUNG": ("silver/temp2", "saoke_tindung.sql.j2"),
}


def to_spark_expr(expr):
    """Chuẩn hoá biểu thức Oracle trong cột Transform sang macro/Spark SQL."""
    m = re.match(r"TO_DATE\(CAST\((\w+\.\w+) AS VARCHAR\(20\)\), 'YYYYMMDD'\)", expr)
    if m:
        return f"{{{{ t24_date('{m.group(1)}') }}}}"
    return expr


for sheet, (folder, tmpl) in STAGING.items():
    sel = []
    seen = set()
    for m in mapping_rows(sheet):
        col = m["Cột đích"]
        if col.upper() in seen:
            raise SystemExit(f"duplicate column {col} in {sheet}")
        seen.add(col.upper())
        sel.append(f"    {to_spark_expr(m['Transform'])} as {col.upper()}")
    template = open(os.path.join(ROOT, "tools", "templates", tmpl), encoding="utf-8").read()
    write(f"dbt/models/{folder}/{tmpl.replace('.j2', '')}",
          template.replace("__SELECT_LIST__", ",\n".join(sel)).replace("__NCOLS__", str(len(sel))))
    print(f"{sheet}: {len(sel)} cột")

# YAML docs cho Silver/Gold
ED = "CDR_DT = DATE'{{ var(\'etl_date\') }}'"
TESTS = {
    # SCD1 / SCD2 (PK + 1 dòng hiệu lực)
    "CST": ("scd1", ["CST_ID"]), "CCY": ("scd1", ["CCY_ID"]), "PD": ("scd1", ["PD_ID"]), "OU": ("scd1", ["OU_ID"]),
    "EMPE": ("scd1", ["EMPE_ID"]), "LMT": ("scd1", ["LMT_ID"]), "CV": ("scd1", ["CL_ID"]),
    "IDV_PRFL": ("scd2", ["CST_ID"]), "IDV_EMP_ST": ("scd2", ["CST_ID"]), "ORG_PRFL": ("scd2", ["CST_ID"]),
    "IP_ALT_IDENTN": ("scd2", ["CST_ID"]), "IP_ELC_ADR": ("scd2", ["CST_ID", "ELC_ADR_TP"]),
    "IP_PST_ADR": ("scd2", ["CST_ID", "PST_ADR_TP"]), "LOAN_AR": ("scd2", ["AR_ID"]), "LOAN_AR_PRFL": ("scd2", ["AR_ID"]),
    "AR_X_IP_RLTNP": ("scd2", ["AR_ID"]), "AR_RATE_HIST": ("scd2", ["AR_ID"]), "AR_AVY_SHD_ITM": ("scd2", ["AR_AVY_SHD_ID"]),
    # DIM: DIM_KEY duy nhất + 1 dòng hiệu lực / ID
    "DIM_LOAN_AR": ("dim", ["AR_ID"]), "DIM_CST": ("dim", ["CST_ID"]), "DIM_PD": ("dim", ["PD_ID"]), "DIM_OU": ("dim", ["OU_ID"]),
    "DIM_CCY": ("dim", ["CCY_ID"]), "DIM_LMT": ("dim", ["LMT_ID"]), "DIM_RSTC_TP": ("dim", ["RSTC_TP_ID"]), "DIM_FNC_ST": ("dim", ["FNC_ST_ID"]),
    # SNAPSHOT / TXN: PK theo ngày chạy
    "EXG_RATE": ("snap", ["SNPST_DT", "FRST_CCY_CODE"], "SNPST_DT"), "AR_BAL": ("snap", ["CDR_DT", "AR_ID", "GL_ITM_CODE", "AST_TP_CODE"], "CDR_DT"),
    "AR_DLQ_SMY": ("snap", ["CDR_DT", "AR_ID"], "CDR_DT"), "AR_TVR_SMY": ("snap", ["CDR_DT", "AR_ID"], "CDR_DT"),
    "AST_AR_INT_SMY": ("snap", ["CDR_DT", "AR_ID"], "CDR_DT"), "PST_ENTR": ("snap", ["PST_ENTR_ID"], "PST_DT"),
    "INF_CST": ("snap", ["CDR_DT", "CST_CODE"], "CDR_DT"), "INTF_LOAN_AR": ("snap", ["CDR_DT", "AR_CODE", "GL_ITM_CODE"], "CDR_DT", "warn"),
    "INTF_LOAN_DSBR": ("snap", ["CDR_DT", "AR_CODE"], "CDR_DT"), "INTF_LOAN_REPYMT_SHD": ("snap", ["CDR_DT", "AR_AVY_SHD_CODE"], "CDR_DT"),
    "FCT_LOAN_AR": ("snap", ["CDR_DT_DIM_KEY", "LOAN_AR_DIM_KEY", "MUD_CODE"], "CDR_DT_DIM_KEY"),
}
STAGING_TECH = {"SK_LICH_TRA_NO": "ETL_DATE", "CUST_INFO_DAILY": "ETL_DATE", "SAOKE_GIAINGAN": "ETL_DATE", "SAOKE_TINDUNG": "ETL_DATE"}
# (b) khớp khoá fn_hash giữa các bảng (>= 99.9%)
MATCH = {
    # warn: nguồn PG_T24CORE.CUSTOMER chỉ chứa ~15% KH của hợp đồng (đã kiểm chứng trực tiếp trên Oracle)
    "LOAN_AR": [("CST_ID", "cst", "CST_ID", "SYS_EXP_DT is null", "SYS_EXP_DT is null", "warn")],
    "AR_BAL": [("AR_ID", "loan_ar", "AR_ID", ED, "SYS_EXP_DT is null")],
    "LOAN_AR_PRFL": [("AR_ID", "loan_ar", "AR_ID", "SYS_EXP_DT is null", "SYS_EXP_DT is null")],
    "FCT_LOAN_AR": [("LOAN_AR_DIM_KEY", "dim_loan_ar", "DIM_KEY", None, "SYS_EXP_DT is null"),
                    ("CST_DIM_KEY", "dim_cst", "DIM_KEY", None, "SYS_EXP_DT is null"),
                    ("PD_DIM_KEY", "dim_pd", "DIM_KEY", None, None)],
}
HASH_COLS = {"CST_ID", "AR_ID", "CCY_ID", "PD_ID", "OU_ID", "EMPE_ID", "LMT_ID", "CL_ID", "PST_ENTR_ID", "ORIG_AR_ID", "AR_AVY_SHD_ID", "GL_ITM_ID"}


def test_yaml(tbl, cols):
    y = ["    data_tests:", f"      - column_contract:", f"          expected: [{', '.join(cols)}]"]
    t = TESTS.get(tbl)
    if t and t[0] == "scd1":
        y += [f"      - unique_key:", f"          columns: [{', '.join(t[1])}]"]
    if t and t[0] == "scd2":
        y += [f"      - unique_key:", f"          columns: [{', '.join(t[1] + ['SYS_EFF_DT'])}]",
              f"      - scd2_single_current:", f"          columns: [{', '.join(t[1])}]", "      - scd2_valid_period"]
    if t and t[0] == "dim":
        y += ["      - unique_key:", "          columns: [DIM_KEY]",
              "      - scd2_single_current:", f"          columns: [{', '.join(t[1])}]"]
    if t and t[0] == "snap":
        w = f"{t[2]} = DATE'{{{{ var(\'etl_date\') }}}}'"
        y += [f"      - unique_key:", f"          columns: [{', '.join(t[1])}]", f"          where: \"{w}\""]
        if len(t) > 3:
            y += [f"          config:", f"            severity: {t[3]}"]
        y += [
              f"      - has_rows:", f"          where: \"{w}\""]
    if tbl in STAGING_TECH:
        y += [f"      - has_rows:", f"          where: \"ETL_DATE = DATE'{{{{ var(\'etl_date\') }}}}'\""]
    return y


for tbl, info in docs_index.items():
    model = tbl.lower()
    y = ["version: 2", "", "models:", f"  - name: {model}",
         f"    description: \"PG_{'SILVER' if info['folder'].startswith('silver') else 'GOLD'}.{tbl} – sheet '{info['sheet']}'\""]
    contract_cols = list(info["columns"].keys()) + (["ETL_DATE"] if tbl in STAGING_TECH or tbl == "SAOKE_TINDUNG" else [])
    y += test_yaml(tbl, contract_cols)
    y += ["    columns:"]
    for c, v in info["columns"].items():
        y += [f"      - name: {c.lower()}", f"        description: \"{v['desc']}\""]
        tests = []
        if c in HASH_COLS and c in [k[0] for k in [(cc,) for cc in info["columns"]]]:
            tests.append("is_md5")
        for m in MATCH.get(tbl, []):
            col, to, field, where, to_where = m[:5]
            if col == c:
                tests.append({"hash_match_rate": {"to": f"ref('{to}')", "field": field, "where": where, "to_where": to_where,
                                                  "severity": m[5] if len(m) > 5 else None}})
        if tests:
            y += ["        data_tests:"]
            for tt in tests:
                if isinstance(tt, str):
                    y += [f"          - {tt}"]
                else:
                    a = tt["hash_match_rate"]
                    y += ["          - hash_match_rate:", f"              to: \"{a['to']}\"", f"              field: {a['field']}"]
                    if a["where"]:
                        y += [f"              where: \"{a['where']}\""]
                    if a["to_where"]:
                        y += [f"              to_where: \"{a['to_where']}\""]
                    if a.get("severity"):
                        y += ["              config:", f"                severity: {a['severity']}"]
    write(f"dbt/models/{info['folder']}/_docs_{model}.yml", "\n".join(y) + "\n")

write("tools/target_columns.json", json.dumps(
    {k: {"sheet": v["sheet"], "columns": list(v["columns"].keys())} for k, v in docs_index.items()}, indent=1))

# --------------------------------------------------------------------------- Job dependence -> Airflow
jobs = []
for r in rows("Job_dependence")[1:]:
    if not r[1] or r[1] == "OBJECT_NAME":
        continue
    jobs.append({
        "object": r[1], "model": r[1].split(".")[1].lower(), "group": r[2],
        "waiting": [w.strip() for w in r[3].split(",") if w.strip()],
        "status": r[4], "schedule": r[5], "sql": r[6], "pic": r[8],
    })
assert len(jobs) == 69, len(jobs)  # Master ghi 68 nhưng sheet có 69 (Silver Temp1 = 13)
# test đối soát Bronze vs nguồn (28 bảng, sinh tĩnh để dbt nhận diện source/ref lúc parse)
parts = []
for tgt, t in bronze_tables.items():
    src_tbl = t["src_table"].split("@")[0].lower()
    parts.append(
        f"select '{tgt}' as tbl, b.n as bronze_rows, s.n as source_rows\n"
        f"from (select count(*) as n from {{{{ ref('{tgt.lower()}') }}}} where ETL_DATE = {{{{ etl_date() }}}}) b,\n"
        f"     (select count(*) as n from {{{{ source('{t['src_schema'].lower()}', '{src_tbl}') }}}}) s\n"
        f"where b.n <> s.n")
write("dbt/tests/recon_bronze_vs_source.sql",
      "-- Wave 0: số dòng PG_BRONZE (ETL_DATE = ${ETL_DATE}) khớp 1:1 với bảng nguồn – 28 bảng (sinh tự động)\n"
      + "\nunion all\n".join(parts) + "\n")
write("airflow/dags/jobs.json", json.dumps(jobs, ensure_ascii=False, indent=1))
print("bronze tables:", len(bronze_tables), "| docs:", len(docs_index), "| jobs:", len(jobs))
