"""
Sinh dữ liệu giả lập (mock) cho 28 bảng nguồn (T24CORE, FLEXBO, LOS_APP, SMY, SOURCE, EBANKING)
đúng 100% cấu trúc cột trong sheet Bronze_Mapping_1_1 / DDL (392 cột).

Dữ liệu nhất quán khoá giữa các hệ thống (LD <-> PD <-> FCC <-> LOS <-> SAOKE) để toàn bộ
join của 69 job đều có kết quả, và có biến động theo ngày để kiểm thử SCD1/SCD2:
  ngày 1 (2025-11-26): dữ liệu khởi tạo
  ngày 2 (+1): KH đổi email/điện thoại/địa chỉ/nhóm nợ, KH mới, HĐ mới giải ngân, đổi lãi suất
  ngày 3 (+2): 1 HĐ tất toán (chuyển sang LD_HIS), 1 KH bị xoá, tỷ giá thay đổi
Ghi ra: /data/mock_source/<etl_date>/<BRONZE_TABLE>.csv

Usage: python generate_mock_source.py --etl-date 2025-11-26 [--out /data/mock_source]
"""
import argparse
import csv
import json
import os
import random
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA = json.load(open(os.path.join(HERE, "source_schema.json"), encoding="utf-8"))
BASE = date(2025, 11, 26)

ap = argparse.ArgumentParser()
ap.add_argument("--etl-date", required=True)
ap.add_argument("--out", default="/data/mock_source")
args = ap.parse_args()
ETL = date.fromisoformat(args.etl_date)
DAY = (ETL - BASE).days  # 0,1,2...
OUT = os.path.join(args.out, args.etl_date)
os.makedirs(OUT, exist_ok=True)

TABLES = {k: [] for k in SCHEMA}


def rnd(*seed):
    return random.Random("|".join(str(s) for s in seed))


def d(x):
    return x.isoformat() if x else None


# ------------------------------------------------------------------ master data
BRANCHES = [("VN0010001", "Hội sở chính"), ("VN0010002", "CN Hà Nội"), ("VN0010003", "CN Hồ Chí Minh"),
            ("VN0010004", "CN Đà Nẵng"), ("VN0010005", "CN Cần Thơ")]
for rid, nm in BRANCHES:
    TABLES["T24CORE_COMPANY"].append({"RECID": rid, "COMPANY_NAME": nm})

OFFICERS = [str(1001 + i) for i in range(10)]
for i, o in enumerate(OFFICERS):
    TABLES["T24CORE_DEPT_ACCT_OFFICER"].append({"RECID": o, "NAME": f"Cán bộ tín dụng {i + 1:02d}"})

for rid, tp in [("1001", "INDIV"), ("1002", "INDIV"), ("2001", "CORP"), ("2002", "SME")]:
    TABLES["T24CORE_SECTOR"].append({"RECID": rid, "CUSTOMER_TYPE": tp})

CCY = {"VND": (1, "704", "Viet Nam Dong"), "USD": (25300 + 15 * DAY, "840", "US Dollar"),
       "EUR": (27400 - 20 * DAY, "978", "Euro"), "JPY": (165.5 + DAY * (0.3 if DAY >= 2 else 0), "392", "Japanese Yen")}
for cid, (rate, num, nm) in CCY.items():
    TABLES["T24CORE_CURRENCY"].append({"ID": cid, "CCY_NAME": nm, "NUMERIC_CCY_CODE": num, "MID_REVAL_RATE": rate,
                                       "CURR_NO": 10 + DAY, "DATE_TIME": d(ETL)})
    for n in range(1, 4):
        TABLES["T24CORE_CURRENCY_HIS"].append({"ID": f"{cid};{n}", "CURR_NO": n,
                                               "DATE_TIME": d(BASE - timedelta(days=30 * (4 - n))),
                                               "MID_REVAL_RATE": round(rate * (0.98 + 0.005 * n), 4) if cid != "VND" else 1})

PRODUCTS = [("P01", "VAY_TIEU_DUNG", "Vay tiêu dùng tín chấp"), ("P02", "VAY_MUA_NHA", "Vay mua nhà"),
            ("P03", "VAY_MUA_OTO", "Vay mua ô tô"), ("P04", "VAY_SXKD", "Vay sản xuất kinh doanh"),
            ("P05", "VAY_VLD", "Vay vốn lưu động"), ("P06", "VAY_DAU_TU", "Vay đầu tư dự án")]
for pid, code, nm in PRODUCTS:
    TABLES["LOS_APP_PRODUCT"].append({"ID": pid, "PRODUCT_CODE": code, "PRODUCT_NAME": nm, "RECID": f"PRD.{pid}"})

PROVINCES = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Can Tho", "Hai Phong", "Nghe An"]

# ------------------------------------------------------------------ customers
N_CUST = 40 + (1 if DAY >= 1 else 0)
customers = []
for i in range(N_CUST):
    rid = str(100001 + i)
    if DAY >= 2 and rid == "100040":  # KH bị xoá khỏi nguồn ngày 3
        continue
    r = rnd("cust", rid)
    indiv = i < 25 or rid == "100041"
    khoi = "INDIV" if indiv else ("SME" if i % 2 else "CORP")
    sector = ("1001" if i % 3 else "1002") if indiv else ("2002" if khoi == "SME" else "2001")
    br = BRANCHES[i % 5][0]
    asset_class = "1" if i % 7 else str(2 + i % 3)
    if rid == "100030" and DAY >= 1:
        asset_class = "3"
    email = f"kh{rid}@mail.vn" if i % 4 else None
    if rid == "100003" and DAY >= 1:
        email = "kh100003.new@mail.vn"
    mobile = f"09{r.randint(10000000, 99999999)}"
    if rid == "100005" and DAY >= 1:
        mobile = "0988000005"
    town = PROVINCES[i % 6] if not (rid == "100010" and DAY >= 1) else "Quang Ninh"
    birth = date(1965 + r.randint(0, 35), r.randint(1, 12), r.randint(1, 28)) if indiv else date(2000 + r.randint(0, 20), r.randint(1, 12), 1)
    cust = {
        "RECID": rid, "KHOI": khoi, "SECTOR": sector, "CO_CODE": br, "COMPANY_BOOK": br, "OPEN_COMPANY": BRANCHES[(i + 1) % 5][0],
        "ACCOUNT_OFFICER": OFFICERS[i % 10], "SECOND_OFFICER": OFFICERS[(i + 3) % 10], "REFERRED_BY": OFFICERS[(i + 5) % 10] if i % 3 == 0 else None,
        "SHORT_NAME": f"KH{rid}", "VN_FULL_NAME": (f"Nguyễn Văn {rid}" if indiv else f"Công ty TNHH {rid}"),
        "VN_GENDER": ("MALE" if i % 2 else "FEMALE") if indiv else None,
        "BIRTH_INCORP_DATE": d(birth), "BDATE_LEGAL": None, "BDAY_DIRECTOR": None if indiv else d(date(1970, 5, 1 + i % 27)),
        "CONTACT_DATE": d(date(2015 + i % 10, 1 + i % 12, 1 + i % 28)), "DATE_TIME": d(ETL), "TXN_DATE": d(ETL),
        "CUSTOMER_STATUS": "1", "ACTIVE_STATUS": "ACTIVE", "ASSET_CLASS": asset_class,
        "AML_CHECK": "Y", "AML_RESULT": "PASS", "INTERNET_BANKING_SERVICE": "Y" if i % 2 else "N", "MOBILE_BANKING_SERVICE": "Y",
        "KYC_CHANNEL": "BRANCH" if i % 3 else "EKYC", "MA_BHXH": f"BHXH{rid}" if indiv else None,
        "NATIONALITY": "VN", "COUNTRY": "VN", "RESIDENCE": "VN", "RESID_PROVINCE": PROVINCES[(i + 2) % 6],
        "NATIONAL_ID": f"0{r.randint(10**10, 10**11 - 1)}" if indiv or i % 2 else None, "NAT_ID_TYPE": "CCCD",
        "NAT_ID_ISS_DATE": d(date(2021, 1 + i % 12, 1 + i % 28)), "NAT_ID_EXP_DATE": d(date(2036, 1 + i % 12, 1 + i % 28)),
        "NAT_ID_ISS_PLC": "CUC CS QLHC", "NO_LIEN_DOI": "N", "TAX_SERIAL": f"0{r.randint(10**8, 10**9 - 1)}",
        "VAT_INV_SERIAL": None, "VAT_INV_DATE": d(date(2020, 1 + i % 12, 1)), "VIP_CUSTOMER": "Y" if i % 9 == 0 else "N",
        "FFB_TYPE": "GOLD" if i % 9 == 0 else None, "CU_EFF_DATE": d(date(2024, 1, 1)) if i % 9 == 0 else None,
        "CUS_IND_PARENT": f"{['A', 'C', 'F', 'G', 'K'][i % 5]}", "MB_INDUSTRY": f"{['A', 'C', 'F', 'G', 'K'][i % 5]}{i % 9 + 10}",
        "INDUSTRY": f"IND{i % 9 + 10}", "OCCUPATION": ["NHAN VIEN VP", "KINH DOANH", "GIAO VIEN", "BAC SI"][i % 4] if indiv else None,
        "EMPLOYMENT_STATUS": "EMPLOYED" if indiv else None, "NET_MONTHLY_IN": (15000000 + 1000000 * (i % 20)) if indiv else None,
        "EMAIL_ADDRESS": email, "HOME_NUMBER": f"024{r.randint(1000000, 9999999)}" if i % 3 == 0 else None, "MOBILE_NUMBER": mobile,
        "STREET": f"{i + 1} Pho Hue", "VN_STREET": f"Số {i + 1} đường Láng", "VN_TOWN": town,
        "ESTAB_LIC_CODE": None if indiv else f"GPKD{rid}", "GEN_DIR_PLC": None if indiv else "Ha Noi",
        "CONTACT_PERSON": None if indiv else f"Giám đốc {rid}", "PHONE_CONT_PER": None if indiv else mobile,
        "TARGET": "MASS" if indiv else "SME", "MAU_HD": "HD01", "MB_RM_BANCHEO": OFFICERS[(i + 7) % 10], "MB_CR_STT": "NORMAL",
    }
    customers.append(cust)
    TABLES["T24CORE_CUSTOMER"].append(cust)
CUST_IDS = [c["RECID"] for c in customers if c["RECID"] != "100041"][:40]

# ------------------------------------------------------------------ limits
limits = []
for i, c in enumerate(customers[:30]):
    if i % 2:
        continue
    lid = f"{c['RECID']}.0010000.01"
    limits.append(lid)
    TABLES["T24CORE_LIMIT"].append({"RECID": lid, "LIMIT_PRODUCT": f"HAN MUC {10000 + i}", "INTERNAL_AMOUNT": 5_000_000_000 + i * 1e8,
                                    "MAXIMUM_TOTAL": 6_000_000_000 + i * 1e8, "ONLINE_LIMIT_DATE": d(date(2025, 1, 1 + i)),
                                    "EXPIRY_DATE": d(date(2027, 1, 1 + i))})

# ------------------------------------------------------------------ loans (LD)
N_LOAN = 30 + (1 if DAY >= 1 else 0)
PD_LOANS = {3, 6, 9, 12, 15, 18, 21, 24}
loans = []
for i in range(1, N_LOAN + 1):
    rid = f"LD2501{i:06d}"
    r = rnd("ld", rid)
    cust = customers[(i - 1) % 38]["RECID"] if i <= 30 else "100041"
    ccy = "USD" if i % 7 == 0 else "VND"
    approve = (r.randint(5, 200) * 10_000_000) if ccy == "VND" else r.randint(20, 200) * 1000
    val = date(2025, 11, 26) if i in (29, 30) else (date(2025, 11, 27) if i == 31 else date(2024 + i % 2, 1 + i % 12, 1 + i % 27))
    if val > ETL:
        continue
    matured = (i == 4 and DAY >= 2)
    outstanding = round(approve * (0.4 + 0.5 * r.random()), 2) if val < BASE else approve
    acc_fcc = f"FCC2501{i:06d}" if i % 3 == 0 else None
    rate = 8.5 + (i % 5) * 0.5
    if i == 2 and DAY >= 1:
        rate = 9.75  # đổi lãi suất -> SCD2 AR_RATE_HIST / LOAN_AR_PRFL không đổi
    lim = limits[i % len(limits)] if i % 2 == 0 else None
    loan = {
        "RECID": rid, "ID": rid, "CUSTOMER_ID": cust, "CURRENCY": ccy, "AMOUNT": outstanding, "APPROVE_AMOUNT": approve,
        "DRAWDOWN_NET_AMT": approve, "EXCG_RATE": CCY[ccy][0], "CATEGORY": "21069" if i % 10 == 0 else "21050",
        "CO_CODE": BRANCHES[i % 5][0], "CURR_NO": 3 + (1 if i == 2 and DAY >= 1 else 0), "DATE_TIME": d(ETL), "COMMIT_ACTION": "A",
        "RECORD_STATUS": None, "STATUS": "CUR", "INPUTTER": f"U{OFFICERS[i % 10]}", "AUTHORISER": f"U{OFFICERS[(i + 1) % 10]}",
        "ACC_FCC": acc_fcc, "FCC_LIMIT": lim if acc_fcc else None, "LIMIT_REFERENCE": lim, "CRA_LD": f"HDTD/{2025}/{i:05d}",
        "FIN_MAT_DATE": d(val + timedelta(days=365 * (1 + i % 5))), "VALUE_DATE": d(val), "ORIG_VAL_DATE": d(val),
        "INT_VALUE_DATE": d(val), "INTEREST_RATE": rate, "INT_RATE_TYPE": "1" if i % 2 else "3", "INTEREST_SPREAD": 3.5 if i % 2 == 0 else 0,
        "PE_RATE": 150, "PS_RATE": 10, "REAL_RATE": rate, "INT_LIQ_ACCT": f"1{rid[2:]}01", "PRIN_LIQ_ACCT": f"1{rid[2:]}02",
        "LOC_TERM": ["S", "M", "L"][i % 3], "LOS_FAC_CODE": f"LOS{i:06d}", "MB_RM_BANCHEO": OFFICERS[(i + 2) % 10],
        "PRODUCTGR_CODE": PRODUCTS[i % 6][1], "USE_OF_LOAN": f"MD{i % 6 + 1:02d}", "USE_OF_LOAN_CHA": f"NMD{i % 3 + 1}",
    }
    loans.append((i, loan, matured))
    if not matured:
        TABLES["T24CORE_LD_LOANS_AND_DEPOSITS"].append(loan)
    # LD_HIS: các version cũ (và version cuối nếu đã tất toán)
    n_his = 2 + (1 if matured else 0)
    for n in range(1, n_his + 1):
        h = {k: loan.get(k) for k in [c["name"] for c in SCHEMA["T24CORE_LD_LOANS_AND_DEPOSITS_HIS"]["columns"]]}
        h.update({"RECID": f"{rid};{n}", "CURR_NO": n, "INTEREST_RATE": rate - 0.25 * (n_his - n),
                  "DATE_TIME": d(val + timedelta(days=30 * n)), "RECORD_STATUS": "MAT" if matured and n == n_his else None})
        TABLES["T24CORE_LD_LOANS_AND_DEPOSITS_HIS"].append(h)

# Hợp đồng đã tất toán từ 2024 (chỉ còn trong LD_HIS); LD2401000005 bị REVE -> phải bị loại
for j in range(1, 6):
    rid = f"LD2401{j:06d}"
    for n in (1, 2):
        TABLES["T24CORE_LD_LOANS_AND_DEPOSITS_HIS"].append({
            "RECID": f"{rid};{n}", "CUSTOMER_ID": CUST_IDS[j], "CURRENCY": "VND", "AMOUNT": 0, "APPROVE_AMOUNT": 100_000_000 * j,
            "DRAWDOWN_NET_AMT": 100_000_000 * j, "EXCG_RATE": 1, "CATEGORY": "21050", "CO_CODE": BRANCHES[j % 5][0], "CURR_NO": n,
            "DATE_TIME": d(date(2024, 6, n)), "COMMIT_ACTION": "A", "RECORD_STATUS": ("REVE" if j == 5 and n == 2 else "MAT"),
            "CRA_LD": f"HDTD/2024/{j:05d}", "FIN_MAT_DATE": d(date(2025, 1, j)), "VALUE_DATE": d(date(2024, 1, j)),
            "ORIG_VAL_DATE": d(date(2024, 1, j)), "INT_VALUE_DATE": d(date(2024, 1, j)), "INTEREST_RATE": 9.0, "INT_RATE_TYPE": "1",
            "INTEREST_SPREAD": 0, "REAL_RATE": 9.0, "INT_LIQ_ACCT": f"10001{j}", "PRIN_LIQ_ACCT": f"10002{j}", "LOC_TERM": "M",
            "MB_RM_BANCHEO": OFFICERS[j], "USE_OF_LOAN": "MD01", "USE_OF_LOAN_CHA": "NMD1"})

active = [(i, l) for i, l, m in loans if not m]

# ------------------------------------------------------------------ PD (quá hạn)
pd_list = []
for i, l in active:
    if i not in PD_LOANS:
        continue
    rid = "PD" + l["RECID"]
    r = rnd("pd", rid)
    due = BASE - timedelta(days=10 + 7 * (i % 6))
    pr_amt = round(l["APPROVE_AMOUNT"] * 0.02, 2)
    in_amt = round(l["APPROVE_AMOUNT"] * 0.006, 2)
    pd_list.append((i, l, rid, pr_amt, in_amt))
    TABLES["T24CORE_PD_PAYMENT_DUE"].append({
        "RECID": rid, "CUSTOMER": l["CUSTOMER_ID"], "FLAG_STATUS": None, "STATUS": "PD", "CREDIT_STATUS": "2" if i % 2 else "3",
        "PAYMENT_DTE_DUE": d(due), "ORIGINAL_MAT": l["FIN_MAT_DATE"], "PE_RATE": 150, "PS_RATE": 10, "PENALTY_RATE": 12.5,
        "SNQH_CHUYENDOI": 5 * (i % 3), "LIMIT_REFERENCE": l["LIMIT_REFERENCE"], "LOS_FAC_CODE": l["LOS_FAC_CODE"],
        "REPAYMENT_ACCT": l["PRIN_LIQ_ACCT"], "INPUTTER": l["INPUTTER"], "AUTHORISER": l["AUTHORISER"], "CRA_LD": l["CRA_LD"],
        "USE_OF_LOAN": l["USE_OF_LOAN"], "USE_OF_LOAN_CHA": l["USE_OF_LOAN_CHA"], "PD_TYPE": "0", "CURR_NO": 2})
    TABLES["T24CORE_PD_PAYMENT_DUE_HIS"].append({"RECID": rid, "CURR_NO": 1, "PD_TYPE": "0"})
    for pt, amt in (("PR", pr_amt), ("IN", in_amt)):
        common = {"RECID": rid, "ACC_FCC": l["ACC_FCC"], "APPROVE_AMOUNT": l["APPROVE_AMOUNT"],
                  "CATEGORY": "21069" if (pt == "PR" and i == 12) else "21050", "CO_CODE": l["CO_CODE"], "CREDIT_STATUS": "2",
                  "CUSTOMER": l["CUSTOMER_ID"], "FINAL_DUE_DATE": l["FIN_MAT_DATE"], "FLAG_STATUS": "LIVE",
                  "ORIGINAL_MAT": l["FIN_MAT_DATE"], "PAY_TYPE": pt, "PD_TYPE": "0", "PENALTY_RATE": 12.5, "PE_RATE": 150,
                  "REPAYMENT_ACCT": l["PRIN_LIQ_ACCT"], "RECORD_STATUS": None}
        TABLES["T24CORE_PD_PAYMENT_DUE_MV"].append({**common, "ID": f"{rid}-{pt}", "COMMIT_ACTION": "A", "CURR_NO": 2,
                                                    "DATE_TIME": d(ETL), "PAYMENT_DTE_DUE": d(due),
                                                    "PAY_AMT_OUTS": amt, "REPAID_AMT": round(amt * 0.1, 2)})
        for n in (1, 2):
            TABLES["T24CORE_PD_PAYMENT_DUE_HIS_MV"].append({**common, "CURR_NO": n, "DATE_TIME": d(due + timedelta(days=n)),
                                                            "PAYMENT_DTE_DUE": d(due), "PAY_AMT_OUTS": amt * (1.1 if n == 1 else 1)})

# ------------------------------------------------------------------ schedule, saving multi
for i, l in active:
    v = date.fromisoformat(l["VALUE_DATE"])
    for m in range(1, 25):
        sd = date(v.year + (v.month - 1 + m) // 12, (v.month - 1 + m) % 12 + 1, min(v.day, 28))
        if sd > ETL + timedelta(days=62):
            break
        TABLES["T24CORE_LD_SCHEDULE_DEFINE_MV"].append({"CONTRACT_ID": l["RECID"], "PREV_SCHED_DATE": d(sd)})
    if i % 4 == 0:
        TABLES["T24CORE_MB_MG_SAVING_MULTI"].append({"LD_ID": l["RECID"], "MB_LD_TYPE": f"KM{i % 3 + 1:02d}",
                                                     "PRODUCTGR_CODE": l["PRODUCTGR_CODE"]})

# ------------------------------------------------------------------ STMT_ENTRY (lịch sử từ 01/11) + SMY_STMT_DAILY
seq = 0
stmt_cols = [c["name"] for c in SCHEMA["T24CORE_STMT_ENTRY"]["columns"]]


def stmt_row(day, ref, tcode, amt, ccy, cust, co, crf, prod_cat, forward=False, value_date=None):
    global seq
    seq += 1
    rate = CCY[ccy][0]
    rid = (("F" if forward else "") + f"{day:%y%m%d}{seq:07d}")
    return {
        "RECID": rid, "ACCOUNTING_DATE": d(day), "ACCOUNT_NUMBER": f"AC{ref[-6:]}", "ACCT_ID": f"AC{ref[-6:]}",
        "AMOUNT_FCY": amt if ccy != "VND" else None, "AMOUNT_LCY": round(amt * rate, 2), "BALANCE_TYPE": "BOOKED",
        "BOOKING_DATE": d(day), "COMMIT_ACTION": "A", "COMMIT_TS": f"{day} 10:{seq % 60:02d}:00", "COMPANY_CODE": co,
        "CONSOL_KEY": f"CK.{prod_cat}.{ccy}", "CRF_PROD_CAT": prod_cat, "CRF_TYPE": crf, "CURRENCY": ccy, "CURR_NO": 1,
        "CUSTOMER_ID": cust, "DATE_TIME": d(day), "DEPARTMENT_CODE": "1", "EXCHANGE_RATE": rate, "NARRATIVE": f"GD {tcode} {ref}",
        "OUR_REFERENCE": ref, "PRODUCT_CATEGORY": prod_cat, "RECORD_STATUS": None, "SYSTEM_ID": "LD" if ref.startswith("LD") else "PD",
        "TRANSACTION_CODE": tcode, "VALUE_DATE": d(value_date or day), "ORIGINAL_AMOUNT": amt, "ORIGINAL_CCY": ccy,
        "ORIG_AMOUNT_LCY": round(amt * rate, 2), "ORIGINAL_EXCH_RATE": rate, "OUTSTANDING_BAL": None,
    }


smy_extra = {"ACCOUNT", "ACCOUNT_ID", "ACCOUNT_NUM", "CO_CODE", "CONTRACT_BAL_ID", "ORIGINAL_ACCT", "PROCESSING_DATE",
             "STMT_ENTRY_ID", "STMT_NO", "THEIR_REFERENCE", "TRANS_REFERENCE", "TXN_DATE"}
day = date(2025, 11, 1)
while day <= ETL:
    for i, l in active:
        v = date.fromisoformat(l["VALUE_DATE"])
        ccy, cust, co, cat = l["CURRENCY"], l["CUSTOMER_ID"], l["CO_CODE"], l["CATEGORY"]
        rows = []
        if v == day:  # giải ngân
            rows.append(stmt_row(day, l["RECID"], "402", l["APPROVE_AMOUNT"], ccy, cust, co, "D", cat))
        if (day.day + i) % 9 == 0 and v < day:  # thu gốc + lãi
            rows.append(stmt_row(day, l["RECID"], "420", round(l["APPROVE_AMOUNT"] * 0.01, 2), ccy, cust, co, "C", cat))
            rows.append(stmt_row(day, l["RECID"], "434", round(l["APPROVE_AMOUNT"] * 0.007, 2), ccy, cust, co, "C", cat))
        if i in PD_LOANS and (day.day + i) % 5 == 0:
            rows.append(stmt_row(day, "PD" + l["RECID"], "751", round(l["APPROVE_AMOUNT"] * 0.001, 2), ccy, cust, co, "C", cat))
            rows.append(stmt_row(day, "PD" + l["RECID"], "750", round(l["APPROVE_AMOUNT"] * 0.002, 2), ccy, cust, co, "C", cat))
        if day == ETL and i % 2 == 1:  # bút toán tương lai (lịch trả nợ) – RECID 'F...'
            nxt = ETL + timedelta(days=15 + i % 10)
            rows.append(stmt_row(day, l["RECID"], "420", round(l["APPROVE_AMOUNT"] * 0.01, 2), ccy, cust, co, "D", cat, True, nxt))
            rows.append(stmt_row(day, l["RECID"], "434", round(l["APPROVE_AMOUNT"] * 0.007, 2), ccy, cust, co, "D", cat, True, nxt))
            if i in PD_LOANS:
                rows.append(stmt_row(day, "PD" + l["RECID"], "750", round(l["APPROVE_AMOUNT"] * 0.002, 2), ccy, cust, co, "D", cat, True, nxt))
        for s in rows:
            TABLES["T24CORE_STMT_ENTRY"].append(s)
            if not s["RECID"].startswith("F") and day == ETL:
                smy = dict(s)
                smy.update({"ACCOUNT": s["ACCOUNT_NUMBER"], "ACCOUNT_ID": s["ACCT_ID"], "ACCOUNT_NUM": s["ACCOUNT_NUMBER"],
                            "CO_CODE": co, "CONTRACT_BAL_ID": l["RECID"], "ORIGINAL_ACCT": s["ACCOUNT_NUMBER"],
                            "PROCESSING_DATE": d(day), "STMT_ENTRY_ID": s["RECID"], "STMT_NO": f"SN{s['RECID'][-6:]}",
                            "THEIR_REFERENCE": l["CRA_LD"], "TRANS_REFERENCE": s["OUR_REFERENCE"], "TXN_DATE": d(day),
                            "REPAYMENT_AMT": s["ORIGINAL_AMOUNT"] if s["TRANSACTION_CODE"] != "402" else None,
                            "OUR_REFERENCE": l["ACC_FCC"] or l["RECID"] if s["TRANSACTION_CODE"] == "402" else s["OUR_REFERENCE"]})
                TABLES["SMY_STMT_DAILY"].append(smy)
                if s["TRANSACTION_CODE"] == "402" and l["ACC_FCC"]:
                    smy2 = dict(smy, RECID=smy["RECID"] + "B", OUR_REFERENCE=l["RECID"])
                    TABLES["SMY_STMT_DAILY"].append(smy2)
    day += timedelta(days=1)

# ------------------------------------------------------------------ SAOKE_CRB / SAOKE_MVMT (số dư cuối ngày)
pd_by_loan = {i: (rid, pr, it) for i, l, rid, pr, it in pd_list}
for i, l in active:
    r = rnd("crb", l["RECID"], DAY)
    ccy = l["CURRENCY"]
    rate = CCY[ccy][0]
    bal = round(l["AMOUNT"] * (1 - 0.003 * DAY), 2)
    acr = round(bal * l["INTEREST_RATE"] / 100 / 12 * (0.3 + 0.5 * r.random()), 2)
    base = {"APP_CODE": "LD", "APP_ID": l["RECID"], "CUS_ID": l["CUSTOMER_ID"], "CO_CODE": l["CO_CODE"], "CURR": ccy,
            "CATEG_CODE": l["CATEGORY"], "ASSET_TYPE": "LOAN", "LOC_TERM": l["LOC_TERM"], "SUB_PRO": l["PRODUCTGR_CODE"],
            "MAT_DATE": l["FIN_MAT_DATE"], "VAL_DATE": l["VALUE_DATE"], "DATE": d(ETL), "TXN_DATE": d(ETL), "ACC_FCC": l["ACC_FCC"]}
    TABLES["SOURCE_SAOKE_CRB"].append({**base, "SBV_CODE": "2111", "AMOUNT_CUR": -bal, "AMOUNT_LCY": -round(bal * rate, 2)})
    TABLES["SOURCE_SAOKE_CRB"].append({**base, "SBV_CODE": "3941", "AMOUNT_CUR": acr, "AMOUNT_LCY": round(acr * rate, 2)})
    TABLES["SOURCE_SAOKE_MVMT"].append({"TRANS_REF": l["RECID"], "SBV_CODE": "3941", "TXN_DATE": d(ETL)})
    TABLES["SOURCE_SAOKE_MVMT"].append({"TRANS_REF": l["RECID"], "SBV_CODE": "2111", "TXN_DATE": d(ETL)})
    if i in pd_by_loan:
        rid, pr, it = pd_by_loan[i]
        pb = {**base, "APP_CODE": "PD", "APP_ID": rid}
        TABLES["SOURCE_SAOKE_CRB"].append({**pb, "SBV_CODE": "2121", "AMOUNT_CUR": -pr, "AMOUNT_LCY": -round(pr * rate, 2)})
        TABLES["SOURCE_SAOKE_CRB"].append({**pb, "SBV_CODE": "941", "AMOUNT_CUR": it, "AMOUNT_LCY": round(it * rate, 2)})

# ------------------------------------------------------------------ LOS / FLEXBO / EBANKING
for i, l in active:
    v = l["VALUE_DATE"]
    pid = PRODUCTS[i % 6][0]
    TABLES["LOS_APP_FACILITY"].append({"ID": f"FAC{i:06d}", "PRODUCT_ID": pid})
    TABLES["LOS_APP_LOAN_DISBURSEMENT"].append({"EXT_REF_NO_1": l["RECID"], "AMND_STATE": "F", "STATUS": "A",
                                                "BOOKING_DATE": v, "FACILITY_ID": f"FAC{i:06d}"})
    indiv = next(c for c in customers if c["RECID"] == l["CUSTOMER_ID"])["KHOI"] == "INDIV"
    td = {"CONTRACT_REF_NO": l["RECID"], "MOV_DATE": v, "SAN_PHAM_CHO_VAY_KHCN": PRODUCTS[i % 3][1] if indiv else None,
          "SAN_PHAM_CHO_VAY_KHDN": None if indiv else PRODUCTS[3 + i % 3][1], "MDVAY_SP": f"SP{i % 5 + 1:02d}",
          "MDVAY_NKT": f"N{i % 8 + 1:02d}", "CAN_BO_DE_XUAT_GN": OFFICERS[i % 10], "TT_PP_DE_XUAT_GN": OFFICERS[(i + 4) % 10],
          "CBTD_QL_KHOAN_VAY": OFFICERS[(i + 2) % 10], "TP_PP_QL_KHOAN_VAY": OFFICERS[(i + 6) % 10],
          "CAP_PHE_DUYET_GNTL": "GIAM DOC CHI NHANH", "CAR_APPLICATION_CODE": f"CAR{i:05d}" if PRODUCTS[i % 6][1] == "VAY_MUA_OTO" else None,
          "LOS_CONTRACT_CODE": f"LOSC{i:06d}", "LOS_CONTRACT_ID": f"{i:06d}", "CO_CAU_NO_MIEN_GIAM_LAI": "N",
          "SO_LAN_CO_CAU": i % 3, "BIEN_DO_THAY_DOI_LS_THEO_HDTD": 3.5 if i % 2 == 0 else None, "BDS_TT11": "Y" if i % 6 == 2 else "N"}
    TABLES["FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE"].append(td)
    if v != d(ETL):
        TABLES["FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE"].append(dict(td, MOV_DATE=d(ETL)))
    if l["ACC_FCC"]:
        f = l["ACC_FCC"]
        for ver in (1, 2):
            TABLES["FLEXBO_PGB_LDTB_CONTRACT_MASTER"].append({"CONTRACT_REF_NO": f, "VERSION_NO": ver, "BOOKING_DATE": v})
        TABLES["FLEXBO_PGB_CONTRACT_UDF_MAP"].append({"CONTRACT_REF_NO": f, "FIELD_NAME": "SO LAN CO CAU", "FIELD_VAL": str(i % 2)})
        TABLES["FLEXBO_PGB_CONTRACT_UDF_MAP"].append({"CONTRACT_REF_NO": f, "FIELD_NAME": "LOAI CHUNG KHOAN", "FIELD_VAL": "CP"})
        TABLES["FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST"].append({"CONTRACT_REF_NO": f, "CT_KHUYEN_MAI_CHO_VAY": f"CTKM{i % 4}" if i % 2 else None,
                                                             "CTKM2": f"CTKM2_{i % 3}" if i % 4 == 0 else None, "MOV_DATE": v})
        TABLES["FLEXBO_PGBLD_RT_CONTRACT_UDFIELD_HIST"].append({"CONTRACT_REF_NO": f, "CO_CAU_NO": str(1 + i % 7),
                                                                "NGAY_HET_COVID19": d(date(2022, 6, 30)) if 1 + i % 7 >= 5 else None})
        TABLES["EBANKING_COL_UDF_VALUE"].append({"COL_REF": f, "FUNCTION_CODE": "CONTRACT_INFOR", "UDF_ID": "NHANNO_BATBUOC",
                                                 "UDF_VALUE": "Y" if i % 2 else "N"})

# ------------------------------------------------------------------ write CSV (đủ toàn bộ cột theo DDL)
for tbl, meta in SCHEMA.items():
    cols = [c["name"] for c in meta["columns"]]
    with open(os.path.join(OUT, f"{tbl}.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        for row in TABLES[tbl]:
            unknown = set(row) - set(cols)
            assert not unknown, f"{tbl}: cột không có trong DDL {unknown}"
            w.writerow(["" if row.get(c) is None else row.get(c) for c in cols])
    print(f"{tbl:45s} {len(TABLES[tbl]):6d} rows")
