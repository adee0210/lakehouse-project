# Training Lakehouse – Medallion Bronze → Silver → Gold (dbt-spark + Iceberg)

Dự án Data Engineer xây dựng kho dữ liệu tín dụng ngân hàng theo **kiến trúc Medallion**, triển khai đầy đủ
**69 job ETL** theo tài liệu BA `docs/Training_Fresher_DeepChecked.xlsx` (48 sheet), chạy local trên Docker
theo mô hình [local-lakehouse](https://github.com/TylerHillery/local-lakehouse) (MinIO + Apache Iceberg + Spark Thrift + dbt-spark),
orchestration bằng Airflow. Nguồn dữ liệu: **Oracle `DBPDB` (schema `PG_*`)**, dữ liệu sau biến đổi của
cả 3 tầng được lưu trên **ổ đĩa local** (`./data/minio`).

```
 Oracle DBPDB (PG_T24CORE, PG_FLEXBO, PG_LOS_APP, PG_SMY, PG_SOURCE, PG_EBANKING)
        │  Spark JDBC (đọc trực tiếp, 28 bảng / 392 cột)
        ▼
 ┌──────────── Spark 3.5 Thrift Server + Iceberg 1.6 (SQL engine của dbt-spark) ────────────┐
 │ Wave 0  PG_BRONZE  28 bảng   1:1 raw mirror + ETL_DATE, INGEST_TMS, SRC_STM_CODE (append/ngày) │
 │ Wave 1  PG_SILVER  13 bảng   Silver Temp 1 (Technical)                                     │
 │ Wave 2  PG_SILVER  15 bảng   Silver Temp 2 (Business: SCD1/SCD2/SNAPSHOT/TXN, UNION ALL)  │
 │ Wave 3  PG_GOLD     8 bảng   DIM_* (SCD2, DIM_KEY)                                          │
 │ Wave 4  PG_GOLD     5 bảng   INF_CST, INTF_LOAN_AR/DSBR/REPYMT_SHD, FCT_LOAN_AR             │
 └───────────────────────────────────────────────────────────────────────────────────────────┘
        │ Iceberg data + metadata (S3FileIO)            │ Iceberg catalog (JDBC)
        ▼                                                ▼
   MinIO  s3://bronze | s3://silver | s3://gold     Postgres DB iceberg_catalog
   (bind mount ./data/minio trên máy local)        (dùng chung với Airflow metadata)
```

---

## 1. Công nghệ & lý do lựa chọn

| Thành phần                               | Phiên bản              | Vai trò                                                                              |
| ---------------------------------------- | ---------------------- | ------------------------------------------------------------------------------------ |
| Apache Spark (Thrift Server, local mode) | 3.5.3                  | SQL engine cho dbt-spark (`method: thrift`)                                          |
| Apache Iceberg                           | 1.6.1 (format v2)      | Table format: MERGE/DELETE (SCD), partition, time-travel                             |
| dbt-core / dbt-spark                     | 1.9                    | 69 model, macro SCD, test chất lượng, comment cột                                    |
| MinIO                                    | 2024.4.18              | Object storage S3, bucket `source/bronze/silver/gold` → `./data/minio`               |
| Postgres                                 | 16                     | Iceberg JDBC catalog + Airflow metadata                                              |
| Airflow                                  | 2.10.4 (LocalExecutor) | DAG 69 job sinh từ sheet `Job_dependence`                                            |
| Oracle JDBC (ojdbc8)                     | 23.5                   | Đọc nguồn Oracle cho tầng Bronze                                                     |
| Trino                                    | 467                    | Query engine thứ 2 (đọc chung Iceberg catalog) + catalog `oracle` để đối chiếu nguồn |
| CloudBeaver                              | 24.3                   | SQL editor trên web, kết nối sẵn tới Trino                                           |
| graphify                                 | 0.9                    | Đồ thị code (`graphify-out/`) để tra cứu nhanh cấu trúc dự án                        |

Khác biệt so với local-lakehouse gốc (có chủ đích, do máy chỉ cấp **4 GB RAM** cho Docker):

- Catalog Iceberg dùng **JDBC catalog trên Postgres** sẵn có thay cho service `iceberg-rest` (bớt 1 JVM).
- Spark chạy **local mode trong chính Thrift Server** (không tách master/worker).
- Airflow webserver + scheduler gộp 1 container, đặt trong profile `airflow` (bật khi cần).

## 2. Cấu trúc thư mục

```
lakehouse-project/
├── docker-compose.yml            # postgres, minio, spark-thrift, dbt | profile airflow: airflow-init, airflow | profile ui: trino, cloudbeaver
├── .env / .env.example           # thông tin Oracle, MinIO, Postgres, SOURCE_MODE
├── docker/
│   ├── spark/                    # Dockerfile (Iceberg + AWS bundle + Postgres/Oracle JDBC), spark-defaults.conf, entrypoint
│   ├── postgres/init.sql         # tạo DB iceberg_catalog
│   ├── airflow/Dockerfile        # Airflow + dbt-spark
│   ├── trino/                    # config.properties, jvm.config, catalog/lakehouse.properties (Iceberg JDBC), catalog/oracle.properties
│   └── cloudbeaver/              # initial-data-sources.conf (2 kết nối Trino khai báo sẵn)
├── docs/Training_Fresher_DeepChecked.xlsx   # tài liệu BA (nguồn sự thật)
├── docs/queries/trino_checks.sql # bộ query kiểm tra mẫu cho CloudBeaver/Trino
├── tools/
│   ├── gen_from_excel.py         # SINH CODE từ Excel: bronze models, YAML comment/test, 4 bảng staging, jobs.json
│   ├── templates/*.sql.j2        # khung FROM/JOIN cho 4 bảng staging nhiều cột
│   ├── check_oracle_sources.py   # đối chiếu 28 bảng / 392 cột trong sheet với Oracle thật
│   └── target_columns.json       # danh sách cột đích 41 bảng Silver/Gold (đọc từ sheet)
├── scripts/
│   ├── source_schema.json        # 28 bảng nguồn / 392 cột (sinh từ sheet)
│   ├── oracle_source_map.json    # override tên bảng nguồn sai trong sheet
│   ├── load_source.py            # đăng ký 28 bảng Oracle (JDBC) hoặc nạp mock vào Spark
│   ├── run_pipeline.sh           # chạy 1 ngày: source -> wave0..4 -> dbt test
│   ├── generate_mock_source.py   # dữ liệu giả lập offline (khi không có VPN/Oracle)
│   ├── reset_lakehouse.py        # DROP PURGE toàn bộ bảng các tầng
│   ├── layer_stats.py            # thống kê số dòng 69 bảng theo ngày chạy
│   ├── dbt_docs.sh               # dbt docs generate + serve :8082 (lineage)
│   └── sql.py                    # chạy nhanh SQL qua Thrift
├── dbt/
│   ├── dbt_project.yml, profiles.yml
│   ├── macros/   common.sql (fn_hash, etl_date, t24_date, bz) | scd.sql (scd1/scd2/dim_scd2)
│   │             business.sql (ldh_latest, tdate, dim_key...) | tests.sql | persist_docs.sql
│   ├── models/bronze/            # 28 model Wave 0 (+ _bronze.yml: sources + comment 392 cột)
│   ├── models/silver/temp1/      # 13 model Wave 1
│   ├── models/silver/temp2/      # 15 model Wave 2
│   │                             #  + stg_*.sql: 5 bảng staging (pg_silver_stg) cho SCD2 nặng:
│   │                             #    LOAN_AR, LOAN_AR_PRFL, AR_X_IP_RLTNP, AR_AVY_SHD_ITM, AR_RATE_HIST(temp1)
│   ├── models/gold/dim|interface|fact/   # 8 + 4 + 1 model Wave 3-4
│   └── tests/                    # đối soát (reconciliation)
├── airflow/dags/dag_lakehouse_daily.py + jobs.json
├── graphify-out/                 # graph.json / graph.html / GRAPH_REPORT.md (graphify, tự cập nhật)
└── data/minio/                   # DỮ LIỆU LAKEHOUSE TRÊN MÁY LOCAL (bucket source/bronze/silver/gold)
```

## 3. Ánh xạ tài liệu BA → code

| Sheet                           | Sinh ra                                                                                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Master                          | Quy ước code: `fn_hash`, `${ETL_DATE}`, SCD1/SCD2/SNAPSHOT/TXN, 3 cột audit, DIM_KEY, anti fan-out, checklist test                                                    |
| Phan_Cong_Task                  | Phân nhóm model theo Track (comment đầu mỗi file SQL)                                                                                                                 |
| Job_dependence                  | `airflow/dags/jobs.json` → 69 task Airflow, phụ thuộc đúng cột WAITING, tag `wave0..wave4` trong dbt                                                                  |
| Bronze_Mapping_1_1 + DDL        | 28 model `models/bronze/*.sql`, `_bronze.yml` (source + COMMENT 392 cột), `scripts/source_schema.json`                                                                |
| Map_SRC2TRG + 4 sheet staging   | `sk_lich_tra_no` (117 cột), `cust_info_daily` (63), `saoke_giaingan` (73), `saoke_tindung` (67) – SELECT list sinh tự động từ cột Transform                           |
| 23 sheet Silver + 13 sheet Gold | 41 model viết tay theo khối Header Join + bảng Mapping; YAML `_docs_<table>.yml` (COMMENT cột + test, gồm **column contract** so khớp đúng thứ tự & số cột với sheet) |
| Tham khảo Interface accouting   | Chỉ tham khảo (ngoài phạm vi 69 job theo Master)                                                                                                                      |

### 69 job theo Wave (đúng sheet `Job_dependence`)

| Wave                     | Lịch trong sheet         | Bảng                                                                                                                                                                                       |
| ------------------------ | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| WAVE_0_BRONZE_1_1 (28)   | `30 0 * * *`             | EBANKING_COL_UDF_VALUE, FLEXBO_* (5), LOS_APP_* (3), SMY_STMT_DAILY, SOURCE_SAOKE_CRB/MVMT, T24CORE_* (16)                                                                                 |
| WAVE_1_SILVER_TEMP1 (13) | `0 1 * * *`              | IP_ALT_IDENTN, IP_ELC_ADR, IP_PST_ADR, CCY, PD, OU, EMPE, LMT, CV, sk_lich_tra_no, EXG_RATE, CUST_INFO_DAILY, AR_RATE_HIST                                                                 |
| WAVE_2_SILVER_TEMP2 (15) | `30 1` / `45 1`          | CST, IDV_PRFL, IDV_EMP_ST, ORG_PRFL, LOAN_AR, LOAN_AR_PRFL, AR_X_IP_RLTNP, AR_AVY_SHD_ITM, Saoke_GIAINGAN, PST_ENTR, SAOKE_TINDUNG, AR_BAL, AR_DLQ_SMY, AR_TVR_SMY (01:45), AST_AR_INT_SMY |
| WAVE_3_GOLD_DIM (8)      | `0 2 * * *`              | DIM_LOAN_AR, DIM_CCY, DIM_PD, DIM_OU, DIM_LMT, DIM_RSTC_TP, DIM_FNC_ST, DIM_CST                                                                                                            |
| WAVE_4_GOLD (5)          | `15 2` / `30 2` / `45 2` | INF_CST, INTF_LOAN_AR, INTF_LOAN_DSBR, INTF_LOAN_REPYMT_SHD, FCT_LOAN_AR                                                                                                                   |

> Master ghi "68 job (12 Silver Temp 1)" nhưng sheet `Job_dependence` thực tế có **69 job (13 Silver Temp 1)** – dự án triển khai đủ 69.

### Quy ước kỹ thuật đã hiện thực (Master – Phần II)

| Quy ước                  | Cách làm                                                                                                                                                                                                                    |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fn_hash("PREFIX", COL)` | macro `fn_hash` = `md5(concat('PREFIX', COL))` – CHAR(32), **không** có `'                                                                                                                                                  | '`, COL NULL → NULL |
| `${ETL_DATE}`            | dbt var `etl_date` (`--vars '{etl_date: YYYY-MM-DD}'`), Airflow truyền `ds` hoặc `dag_run.conf.etl_date`                                                                                                                    |
| Bronze 1:1 + audit       | `SELECT src.*, ETL_DATE, INGEST_TMS, SRC_STM_CODE`, phân vùng `ETL_DATE`, chạy lại cùng ngày: delete-partition + append (idempotent)                                                                                        |
| Đọc Bronze ở Silver      | macro `bz()` luôn lọc `ETL_DATE = ${ETL_DATE}`                                                                                                                                                                              |
| SCD TYPE 1               | macro `scd1`: MERGE theo PK, giữ `SYS_EFF_DT` lần đầu, `SYS_UDT_DT` = ngày thay đổi, mất khỏi nguồn → `SYS_EXP_DT`                                                                                                          |
| SCD TYPE 2               | macro `scd2`: đổi thuộc tính → đóng dòng cũ (`SYS_EXP_DT=${ETL_DATE}`) + mở dòng mới; 1 dòng hiệu lực / business key; chạy lại idempotent                                                                                   |
| SNAPSHOT / TXN           | partition `CDR_DT` / `SNPST_DT` / `PST_DT` – xoá & ghi lại phân vùng ngày chạy                                                                                                                                              |
| Gold DIM                 | macro `dim_scd2`: `DIM_KEY = CONCAT(TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD'), ID)`, `ETL_CHANGE_DATE = COALESCE(SYS_UDT_DT, SYS_EFF_DT, ${ETL_DATE})`                                                                            |
| SCD2 nhiều nhánh         | Bảng 3 nhánh UNION ALL / nhiều JOIN tách thành `pg_silver_stg.stg_<bảng>` (materialized table) → model SCD2 chỉ MERGE từ bảng staging (kế hoạch MERGE nhỏ, không OOM). Airflow chạy `stg_<bảng>` + `<bảng>` trong cùng task |
| Anti fan-out             | join SCD2/DIM luôn kèm `SYS_EXP_DT IS NULL`; AR_BAL distinct `(AR_ID, CDR_DT, MUD_CODE)`; subquery PST_ENTR/AR_DLQ/AST gom trước; số ngày quá hạn dùng MAX                                                                  |
| COMMENT cột              | YAML từ sheet + `persist_docs` (override chỉ ALTER khi comment thay đổi)                                                                                                                                                    |
| Ngày T24                 | macro `t24_date`: hỗ trợ `'YYYYMMDD'`, NUMBER `20320105`, `DATE_TIME` `'YYMMDDHH24MI'`, `'YYYY-MM-DD hh:mm:ss'`, đa trị `'20260215#20260115#…'` (lấy giá trị đầu như spec `SUBSTR(...,1,8)`)                                |

## 4. Kiểm chứng nguồn Oracle (`tools/check_oracle_sources.py`)

Quy ước: bảng nguồn = `PG_` + schema trong sheet (vd `T24CORE.CUSTOMER` → `PG_T24CORE.CUSTOMER`, bỏ DB link `@PGDB/@TO_LOS`).
Kết quả đối chiếu 28 bảng / 392 cột với Oracle `DBPDB`:

| Bảng trong sheet                    | Thực tế Oracle                                                                                                                                                                        | Xử lý                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `SMY.STMT_DAILY`                    | **không tồn tại** – đúng bảng là `PG_SMY.STMT_ENTRY_LOAN_GRA` (khớp 56/56 cột)                                                                                                        | override trong `scripts/oracle_source_map.json` |
| `T24CORE.CUSTOMER` (63 cột)         | `PG_T24CORE.CUSTOMER` chỉ có 10 cột (RECID, ASSET_CLASS, KHOI, SECTOR, VN_FULL_NAME, MOBILE_NUMBER, CUS_IND_PARENT, MB_INDUSTRY…); không có bảng nào khác trên DB đủ cột & có dữ liệu | 55 cột thiếu → `CAST(NULL AS <kiểu DDL>)`       |
| `T24CORE.LIMIT` (6 cột)             | chỉ có `RECID`, `FCC_LIMIT`                                                                                                                                                           | 5 cột thiếu → NULL                              |
| `LOS_APP.PRODUCT`                   | thiếu `RECID`                                                                                                                                                                         | NULL                                            |
| `SOURCE.SAOKE_CRB`                  | thiếu cột `DATE`                                                                                                                                                                      | NULL                                            |
| `T24CORE.LD_LOANS_AND_DEPOSITS`     | thiếu `ID`                                                                                                                                                                            | NULL                                            |
| `T24CORE.LD_LOANS_AND_DEPOSITS_HIS` | thiếu `COMMIT_ACTION`                                                                                                                                                                 | NULL                                            |
| `T24CORE.PD_PAYMENT_DUE`            | thiếu `FLAG_STATUS`                                                                                                                                                                   | NULL                                            |
| `T24CORE.PD_PAYMENT_DUE_MV`         | thiếu `COMMIT_ACTION`, `ID`                                                                                                                                                           | NULL                                            |
| 19 bảng còn lại                     | khớp 100%                                                                                                                                                                             | –                                               |

Đặc điểm dữ liệu thật & xử lý:

- Nhiều cột số/ngày lưu `VARCHAR2`; cột `NUMBER` không khai báo precision được đọc **dạng text nguyên bản**
  (`customSchema`) để tránh `1000385248.0000000000` làm lệch khoá join (`CUSTOMER.RECID` NUMBER ↔ `LD.CUSTOMER_ID` VARCHAR2).
- `STMT_ENTRY.COMMIT_ACTION` toàn NULL → lọc `COMMIT_ACTION <> 'D'` theo nghĩa đen sẽ loại hết dòng;
  dùng `NVL(COMMIT_ACTION,'_') <> 'D'` (giữ ý định "loại bút toán bị xoá"). Tương tự `FLAG_STATUS <> 'DELETE'`.
- Số tiền lưu `VARCHAR2` (vd `PAY_AMT_OUTS`, `AMOUNT_LCY`): luôn **ép `DECIMAL(25,5)` tường minh** trước khi so sánh/SUM;
  nếu để Spark ép ngầm (`AMOUNT_LCY < 0`) chuỗi bị chuyển sang `INT` → số > 2,1 tỷ tràn thành NULL và rơi khỏi tổng
  (đã gặp: FCT chỉ ra 61,8 tỷ thay vì 1.071 tỷ trước khi sửa). `CURR_NO` dạng chuỗi được ép `INT` khi lấy MAX.
- Ngày nghiệp vụ có dữ liệu: **2025-10-07** (SAOKE_CRB / SAOKE_MVMT / STMT_ENTRY) → `ETL_DATE` mặc định.

## 5. Các điểm sửa / làm rõ so với tài liệu BA

| Sheet                                  | Vấn đề                                                                                                                                                                             | Cách xử lý                                                                                                                                                          |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Arrangement Turnover Summary           | Cột Transform của 12 dòng `TOT_ODUE_PNP_AMT_FCY … NBR_OF_DB_IN_YR` bị **lệch 1-2 dòng** so với cột đích                                                                            | Map theo tên cột đích + "Bảng/Cột nguồn" của chính dòng (vd `TOT_ODUE_PNP_AMT_FCY ← AR_DLQ_SMY.PNP_ARS`)                                                            |
| Interface Loan Arrangement             | Join `AR_RATE_HIST d` ghi `a.AR_ID = e.AR_ID AND e.CDR_DT` (sai alias, bảng SCD2 không có CDR_DT)                                                                                  | `a.AR_ID = d.AR_ID AND d.SYS_EXP_DT IS NULL`                                                                                                                        |
| Interface Loan Arrangement             | Join `AR_DLQ_SMY e` gây fan-out với HĐ gốc LD                                                                                                                                      | `e` chỉ join cho dòng `PDLD%` (đúng ghi chú), HĐ LD dùng `e1` đã GROUP BY                                                                                           |
| Arrangement Balance                    | Header không có điều kiện ngày                                                                                                                                                     | `TXN_DATE = ${ETL_DATE}` theo Master                                                                                                                                |
| Loan Arrangement                       | `FRST_OU_CODE`                                                                                                                                                                     | Theo ghi chú: FIRST_VALUE(CO_CODE) trên tập hợp nhất LD ∪ LD_HIS                                                                                                    |
| LOAN_AR_PRFL / staging                 | Bảng phụ nhiều dòng/khoá (UDF hist, MVMT, schedule, PD_MV…)                                                                                                                        | lấy 1 dòng mới nhất / khoá trước khi LEFT JOIN để không trùng PK                                                                                                    |
| 4 bảng staging, SAOKE_TINDUNG          | Không có cột ngày snapshot                                                                                                                                                         | bổ sung cột kỹ thuật `ETL_DATE` làm partition                                                                                                                       |
| PST_ENTR/AR_AVY_SHD/sk_lich_tra_no/AST | `COMMIT_ACTION <> 'D'`, `FLAG_STATUS <> 'DELETE'` loại cả NULL                                                                                                                     | `NVL(...,'_')`                                                                                                                                                      |
| Master                                 | 68 job                                                                                                                                                                             | sheet thực tế 69 job                                                                                                                                                |
| Loan Arrangement                       | `PD_CODE`: HĐ có `ACC_FCC` lấy `TDATE.SAN_PHAM_CHO_VAY_KHCN/KHDN`, nhưng TDATE thật chỉ có giá trị ở ~7% HĐ → **100% dòng FCT rỗng `PD_DIM_KEY`** (phát hiện khi query bằng Trino) | `NVL(<CASE spec>, NVL(PRODUCT_CODE, PRODUCTGR_CODE))` – rơi về nhánh ELSE của chính spec. FCT: PD 0 → 1,580/1,812 dòng. Test cảnh báo mới `dq_fct_dim_key_coverage` |

## 6. Cài đặt & chạy

### 6.0 Chạy nhanh: 1 lệnh `start.ps1` (Windows PowerShell)

Bật Docker Desktop + VPN, rồi:

```powershell
cd C:\Users\ADMIN\lakehouse-project
.\start.ps1                       # bật toàn bộ hệ thống + dbt docs + chạy pipeline 69 job, theo dõi đến khi xong
.\start.ps1 -EtlDate 2025-10-07   # chạy cho 1 ngày nghiệp vụ cụ thể (mặc định: Airflow Variable etl_date)
.\start.ps1 -From 2025-10-08 -To 2025-10-09                    # nhiều ngày: TUẦN TỰ từng ngày (SCD2 cần đúng thứ tự)
.\start.ps1 -From 2025-10-08 -To 2025-10-09 -ContinueOnError   # ngày lỗi vẫn chạy tiếp ngày sau
.\start.ps1 -NoRun                # chỉ bật hệ thống + giao diện, không chạy pipeline (không cần VPN)
.\start.ps1 -NoWait               # trigger pipeline rồi thoát ngay
.\start.ps1 -Build                # rebuild image sau khi sửa thư mục docker/
.\start.ps1 -Stop                 # tắt toàn bộ container (dữ liệu giữ nguyên)
# Nếu bị chặn script: powershell -ExecutionPolicy Bypass -File .\start.ps1
```

Script làm lần lượt: mở Docker Desktop nếu chưa chạy → kiểm tra kết nối Oracle (VPN) → `docker compose --profile airflow
--profile ui up -d` → chờ Airflow healthy → `dbt docs` (:8082) → trigger DAG `dag_lakehouse_daily` (mở trang Graph
trên trình duyệt) → in tiến độ mỗi 20 s đến khi xong, liệt kê task lỗi nếu có. Thời gian trên máy 16 GB: ~8 phút.

> Không dùng `docker compose down -v`: `-v` xoá volume Postgres (Iceberg catalog + lịch sử Airflow).

**Chạy nhiều ngày:** mỗi ngày là 1 DAG run riêng (conf `etl_date`), ngày sau chỉ bắt đầu khi ngày trước xong. Bronze đọc
trạng thái Oracle _tại thời điểm chạy_ (không lọc theo ngày), Silver/Gold lọc theo ngày nghiệp vụ. Trên Oracle hiện tại
SAOKE (số dư) chỉ có ngày 2025-10-07, nên chạy 2025-10-08 cho kết quả: 69/69 job thành công, `dbt test` 198 PASS ·
8 ERROR – toàn bộ là `has_rows` (AR_BAL, SAOKE_GIAINGAN, AR_TVR_SMY, AST_AR_INT_SMY, INTF_LOAN_AR, FCT_LOAN_AR,
PST_ENTR, INTF_LOAN_REPYMT_SHD rỗng). `has_rows` cố ý để mức error: chạy hằng ngày mà bảng snapshot rỗng = nguồn chưa về.
Lịch Airflow `30 0 * * *` chạy 1 lần/ngày khi máy bật (`catchup=False`: không chạy bù ngày đã lỡ). Khi Oracle được cập
nhật hằng ngày, xoá Variable `etl_date` (Admin → Variables) để mỗi run tự lấy ngày T-1 (`ds`).

### 6.1 Chuẩn bị

- Docker Desktop (≥ 4 GB RAM cho Docker), Git Bash/PowerShell.
- Kết nối VPN tới Oracle `192.168.26.180:1521/DBPDB` (container Spark đi ra ngoài qua mạng của máy).
- `.env` (xem `.env.example`): `ORACLE_*`, `MINIO_ROOT_*`, `POSTGRES_*`, `SOURCE_MODE=oracle`.
- `AIRFLOW_FERNET_KEY` phải là khoá Fernet hợp lệ (Variable/Connection được mã hoá bằng khoá này):
  `python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"`.

**Tài nguyên theo máy** – mọi giới hạn đọc từ `.env`, không cần sửa code (bỏ trống = mặc định cho Docker 4 GB):

| Biến                                                                       | Mặc định (Docker 4 GB) | Máy 16 GB RAM (Docker 8 GB) |
| -------------------------------------------------------------------------- | ---------------------- | --------------------------- |
| `SPARK_CORES` / `SPARK_DRIVER_MEMORY` / `SPARK_MEM_LIMIT`                  | 4 / 1600m / 2300m      | 6 / 3g / 4g                 |
| `DBT_THREADS` (dbt `threads`)                                              | 2                      | 4                           |
| `AIRFLOW_PARALLELISM` (= `max_active_tasks` của DAG) / `AIRFLOW_MEM_LIMIT` | 2 / 1200m              | 4 / 1800m                   |
| `ETL_DATE` (Airflow Variable `etl_date`)                                   | 2025-10-07             | 2025-10-07                  |

**Chuyển sang máy khác:** `git clone/pull` → kiểm tra `.env` → `docker compose --profile airflow --profile ui up -d --build`
→ bật DAG (`airflow dags unpause dag_lakehouse_daily`). Dữ liệu lakehouse (`data/minio`) và metadata (volume Postgres)
**không nằm trong git**, nên máy mới bắt đầu từ lakehouse rỗng và cần chạy lại DAG một lần (~15 phút với cấu hình 16 GB).

### 6.2 Khởi động lakehouse

```bash
docker compose up -d --build postgres minio spark-thrift dbt
# MinIO console: http://localhost:9001  (minioadmin / minioadmin123) | Spark UI: http://localhost:4040
```

### 6.3 Kiểm tra nguồn Oracle (tuỳ chọn, chạy trên host)

```bash
pip install oracledb openpyxl
python tools/check_oracle_sources.py          # đối chiếu 28 bảng / 392 cột
```

### 6.4 Chạy pipeline 1 ngày (Source → Wave 0..4 → test)

```bash
# Git Bash cần MSYS_NO_PATHCONV=1 để không đổi đường dẫn /usr/app
MSYS_NO_PATHCONV=1 docker compose exec dbt sh /usr/app/scripts/run_pipeline.sh 2025-10-07 oracle
```

Hoặc từng bước:

```bash
docker compose exec dbt python /usr/app/scripts/load_source.py --etl-date 2025-10-07 --mode oracle
docker compose exec dbt dbt run  -s tag:wave0 --vars '{etl_date: 2025-10-07}'   # Bronze
docker compose exec dbt dbt run  -s tag:wave1 --vars '{etl_date: 2025-10-07}'   # Silver Temp 1
docker compose exec dbt dbt run  -s tag:wave2 --vars '{etl_date: 2025-10-07}'   # Silver Temp 2
docker compose exec dbt dbt run  -s tag:wave3 --vars '{etl_date: 2025-10-07}'   # Gold DIM
docker compose exec dbt dbt run  -s tag:wave4 --vars '{etl_date: 2025-10-07}'   # Gold INF/INTF/FCT
docker compose exec dbt dbt test --vars '{etl_date: 2025-10-07}'
docker compose exec dbt dbt run -s +fct_loan_ar --vars '{etl_date: 2025-10-07}' # 1 bảng + toàn bộ upstream
```

### 6.5 Chạy bằng Airflow

```bash
docker compose --profile airflow up -d --build      # http://localhost:8081  (airflow / airflow)
docker compose exec airflow airflow dags unpause dag_lakehouse_daily   # DAG mới tạo luôn ở trạng thái pause
# Ngày nghiệp vụ = conf {"etl_date": ...} > Variable etl_date (airflow-init đặt = ETL_DATE trong .env) > ds
docker compose exec airflow airflow dags trigger dag_lakehouse_daily -c '{"etl_date": "2025-10-07"}'   # chạy tay 1 ngày bất kỳ
# hoặc trên UI: nút ▶ "Trigger DAG w/ config"; đổi ngày mặc định: Admin → Variables → etl_date
```

DAG `dag_lakehouse_daily` (73 task = `prepare_source` + 69 job + `dbt_test_data_quality` + `report_layer_row_counts` + `end`):

- TaskGroup theo `GRP_OBJ` (WAVE_0 … WAVE_4); mỗi job = `dbt run -s <model>`.
- Phụ thuộc task-level đúng cột **WAITING** (vd `FCT_LOAN_AR` chờ 18 bảng; `AR_TVR_SMY` chờ `PST_ENTR, AR_DLQ_SMY, LOAN_AR`).
- Lịch `30 0 * * *` (Wave 0); các mốc 01:00–02:45 của sheet được thay bằng dependency thực (job chạy ngay khi upstream xong).

> Với 4 GB RAM, nên **tắt Airflow khi chạy tay bằng `run_pipeline.sh`** (Spark 2.3 GB + MinIO 768 MB + Postgres + Airflow 1.2 GB gần chạm giới hạn).
> Độ song song được giới hạn có chủ đích: dbt `threads: 2`, Airflow `parallelism=2` / `max_active_tasks=2` (Spark local mode dùng chung 1 heap 1,6 GB).
> **Không chạy 2 pipeline cùng lúc** (2 lần `run_pipeline.sh` hoặc pipeline tay + DAG) – các model incremental cùng ghi 1 bảng sẽ xung đột và tranh bộ nhớ.

### 6.6 Chế độ offline (không có VPN)

`SOURCE_MODE=mock`: `generate_mock_source.py` sinh dữ liệu giả lập đúng 392 cột, nhất quán khoá giữa các hệ thống và
có biến động theo ngày (KH đổi email/địa chỉ/nhóm nợ, HĐ mới, HĐ tất toán…) để test SCD1/SCD2:

```bash
MSYS_NO_PATHCONV=1 docker compose exec dbt sh /usr/app/scripts/run_pipeline.sh 2025-11-26 mock
```

### 6.7 Sinh lại code khi tài liệu BA thay đổi

```bash
python tools/gen_from_excel.py     # bronze models, YAML comment/test, 4 bảng staging, jobs.json, test đối soát Bronze
```

### 6.8 Giao diện theo dõi & truy vấn (Trino + CloudBeaver + dbt docs)

```bash
docker compose --profile ui up -d                                          # Trino + CloudBeaver
MSYS_NO_PATHCONV=1 docker compose exec dbt bash /usr/app/scripts/dbt_docs.sh 2025-10-07   # dbt docs
```

| Giao diện                        | URL                   | Đăng nhập                                              | Dùng để                                                                                                   |
| -------------------------------- | --------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| **Airflow**                      | http://localhost:8081 | `airflow` / mật khẩu Postgres trong `.env`             | Xem 69 job theo Wave/WAITING (Graph), log từng job, task `report_layer_row_counts` in số dòng 3 tầng      |
| **CloudBeaver** (SQL editor web) | http://localhost:8978 | vào thẳng (ẩn danh); admin: `cbadmin` / `Lakehouse123` | Duyệt cây schema `pg_bronze/pg_silver/pg_gold`, xem dữ liệu dạng lưới, chạy SQL, export CSV/Excel         |
| **Trino Web UI**                 | http://localhost:8080 | user bất kỳ (vd `admin`)                               | Theo dõi query đang chạy / lịch sử / plan                                                                 |
| **dbt docs**                     | http://localhost:8082 | –                                                      | **Lineage graph** 74 model (nút xanh góc phải dưới), mô tả bảng/cột lấy từ sheet BA, SQL đã compile, test |
| **MinIO console**                | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`              | File Parquet + metadata Iceberg vật lý của từng bảng (`bronze/ silver/ gold/`)                            |
| **Spark UI**                     | http://localhost:4040 | –                                                      | Job/stage Spark khi dbt đang chạy, tab _JDBC/ODBC Server_ xem câu SQL dbt gửi                             |

**Trino** (`docker/trino/`) dùng chung Iceberg JDBC catalog với Spark (`catalog-name=lakehouse` trên Postgres
`iceberg_catalog`, dữ liệu MinIO) nên thấy ngay các bảng dbt-spark vừa ghi, không phải copy dữ liệu. Có 2 catalog:

| Catalog Trino | Nội dung                                                                                                             |
| ------------- | -------------------------------------------------------------------------------------------------------------------- |
| `lakehouse`   | `pg_bronze`, `pg_silver`, `pg_silver_stg`, `pg_gold` (Iceberg) – vd `SELECT * FROM lakehouse.pg_gold.fct_loan_ar`    |
| `oracle`      | Nguồn thật `PG_*` qua VPN – vd `SELECT count(*) FROM oracle.pg_t24core.ld_loans_and_deposits` → đối chiếu với Bronze |

CloudBeaver được khai báo sẵn 2 kết nối (`docker/cloudbeaver/initial-data-sources.conf`):
_Trino - Lakehouse_ và _Trino - Oracle nguồn_. Bộ câu query kiểm tra mẫu: **`docs/queries/trino_checks.sql`**
(lịch sử snapshot Iceberg, Bronze vs Oracle, SCD2 single-current, đối soát dư nợ 3 tầng, star-schema FCT × DIM).

Kiểm tra yêu cầu sheet trên Airflow: mở DAG `dag_lakehouse_daily` → tab **Graph** (6 TaskGroup = GRP_OBJ, cạnh = cột
WAITING) → bấm 1 task → **Task Instance Details / Rendered Template**: phần _doc_ hiển thị bảng thông tin sheet
`Job_dependence` (GRP_OBJ, STATUS, SCHEDULE, SQL_EXC, PIC, WAITING), tóm tắt yêu cầu nghiệp vụ (header của model SQL)
và câu SQL Trino để xem dữ liệu bảng đó.

Truy vấn qua dòng lệnh:

```bash
docker compose exec trino trino --execute "select * from lakehouse.pg_gold.dim_pd limit 10"
docker compose exec dbt python /usr/app/scripts/sql.py "select * from pg_silver.loan_ar.history"   # Spark, Iceberg metadata
```

Dữ liệu vật lý: `./data/minio/{bronze,silver,gold}/<table>/{data,metadata}`.

> RAM: Trino (~0,9 GB) + CloudBeaver (~0,25 GB) cộng với Spark đang chạy pipeline (~2 GB) vượt 4 GB của Docker.
> Khi chạy pipeline nên `docker compose stop trino cloudbeaver`, chạy xong `docker compose --profile ui up -d`.
> Hoặc nâng RAM cho Docker (WSL2) trong `%USERPROFILE%\.wslconfig`: `[wsl2]` / `memory=6GB` rồi `wsl --shutdown`.

## 7. Kiểm thử chất lượng dữ liệu (Master – Bước 6)

`dbt test` – 206 test (sinh tự động từ sheet + test đối soát viết tay trong `dbt/tests/`):

| Nhóm               | Test                                                                                                                                                                      | Mức                                        |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| (a) Không trùng PK | `unique_key` cho bảng Silver/Gold (bảng snapshot lọc phân vùng ngày chạy). AR_BAL: grain thật `(CDR_DT, AR_ID, GL_ITM_CODE, AST_TP_CODE)`                                 | error (INTF_LOAN_AR: warn, xem 8.3)        |
| SCD2               | `scd2_single_current` (đúng 1 dòng `SYS_EXP_DT IS NULL`/business key), `scd2_valid_period`; DIM: `DIM_KEY` duy nhất                                                       | error                                      |
| (b) Khoá fn_hash   | `is_md5` cho mọi cột *_ID; `hash_match_rate` ≥ 99,9% (AR_BAL→LOAN_AR, LOAN_AR_PRFL→LOAN_AR, FCT→DIM_LOAN_AR/DIM_CST/DIM_PD)                                               | error; LOAN_AR→CST: warn (độ đầy đủ nguồn) |
| (c) Đối soát tiền  | `recon_intf_loan_ar_vs_ar_bal` (INTF = AR_BAL), `recon_fct_loan_ar_vs_ar_bal` (FCT = AR_BAL trên tập HĐ của FCT), `recon_dlq_sum_not_min` (PNP_ARS = SUM, không phải MIN) | error                                      |
| Độ đầy đủ nguồn    | `recon_fct_excluded_balance`: dư nợ của HĐ không vào được FCT vì thiếu KH                                                                                                 | warn                                       |
| Bronze 1:1         | `recon_bronze_vs_source`: số dòng 28 bảng Bronze = bảng nguồn Oracle                                                                                                      | error                                      |
| Cấu trúc           | `column_contract`: tên, thứ tự, số cột của 41 bảng Silver/Gold khớp đúng sheet BA                                                                                         | error                                      |
| Có dữ liệu         | `has_rows` cho bảng snapshot/staging của ngày chạy                                                                                                                        | error                                      |

## 8. Kết quả chạy

Môi trường: Docker Desktop 4 GB RAM, 8 vCPU (Windows 11). Nguồn: Oracle `DBPDB` qua VPN. `ETL_DATE = 2025-10-07`.

### 8.1 Thời gian & trạng thái (`run_pipeline.sh 2025-10-07 oracle`)

| Bước                                   |          Model | Kết quả                         |   Thời gian |
| -------------------------------------- | -------------: | ------------------------------- | ----------: |
| Source – đăng ký 28 bảng Oracle (JDBC) |             28 | OK                              |       ~20 s |
| Wave 0 – PG_BRONZE                     |             28 | **28/28 PASS**                  |        49 s |
| Wave 1 – Silver Temp 1                 | 13 + 1 staging | **14/14 PASS**                  |        39 s |
| Wave 2 – Silver Temp 2                 | 15 + 4 staging | **19/19 PASS**                  |        79 s |
| Wave 3 – Gold DIM                      |              8 | **8/8 PASS**                    |        39 s |
| Wave 4 – Gold INF/INTF/FCT             |              5 | **5/5 PASS**                    |        38 s |
| `dbt test`                             |       206 test | **203 PASS · 3 WARN · 0 ERROR** | 6 phút 20 s |

**Chạy lại qua Airflow** (run `manual_oracle_20251007_v2`, conf `{"etl_date": "2025-10-07"}`, 04/11/2026):

| Bước                                       |     Task | Kết quả                          |    Thời gian |
| ------------------------------------------ | -------: | -------------------------------- | -----------: |
| `prepare_source`                           |        1 | success                          |         34 s |
| WAVE_0 … WAVE_4 (69 job, 2 task song song) |       69 | **69/69 success**                |     ~16 phút |
| `dbt_test_data_quality`                    | 207 test | **203 PASS · 4 WARN · 0 ERROR**  |  6 phút 40 s |
| `report_layer_row_counts`                  |        1 | success (bảng số dòng trong log) |          9 s |
| **Tổng**                                   |       73 | **73/73 success**                | **~23 phút** |

**Máy 16 GB RAM** (Docker 8 GB, cấu hình bảng 6.1, lakehouse rỗng ban đầu): **73/73 success trong 5 phút 16 s**
(69 job ~4,5 phút với 4 task song song, `dbt test` 18 s) – kết quả test và đối soát giống hệt máy 4 GB.

Qua Airflow chậm hơn chạy tay theo wave vì mỗi task khởi động 1 tiến trình dbt riêng (~20 s parse + kết nối),
đổi lại thấy được từng job, log và retry riêng.

Chạy lại cùng ngày cho kết quả không đổi (idempotent): Bronze/Snapshot xoá rồi ghi lại phân vùng; SCD MERGE không sinh
phiên bản mới khi dữ liệu không đổi (lần chạy thứ 2: mọi bảng SCD2 vẫn có số dòng = số dòng hiện hành, vd LOAN_AR 54,483/54,483).

### 8.2 Đối soát dư nợ (Master – Bước 5 & 6)

| Chỉ tiêu (ETL_DATE 2025-10-07)                                                                                 |                                                 Giá trị (VND) |
| -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------: |
| PG_SILVER.AR_BAL – dư nợ gốc quy đổi (GL `2%`, < 0)                                                            |                                         **1,072,809,438,195** |
| PG_GOLD.INTF_LOAN_AR – SUM(AMT_LCY)                                                                            |                            **1,072,809,438,195** ✅ khớp 100% |
| PG_SILVER.AR_TVR_SMY – SUM(CLS_BAL_LCY)                                                                        |                            **1,072,809,438,195** ✅ khớp 100% |
| PG_GOLD.FCT_LOAN_AR – SUM(OTSND_AMT_LCY)                                                                       |     1,071,027,260,932 ✅ khớp 100% trên tập HĐ hợp lệ của FCT |
| Chênh lệch FCT = dư nợ 2 HĐ có KH không tồn tại trong `PG_T24CORE.CUSTOMER` (FCT inner join DIM_CST theo spec) | 1,782,177,263 ⚠ (test `recon_fct_excluded_balance`, mức warn) |

### 8.3 Bốn cảnh báo (WARN) – đều do dữ liệu nguồn, đã kiểm chứng trực tiếp trên Oracle

| Test                                                     | Nguyên nhân                                                                                                                                                                                               |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hash_match_rate` LOAN_AR.CST_ID → CST                   | Trên Oracle: 48,195 HĐ LD, chỉ 28,010 có CUSTOMER_ID và chỉ 4,903 tồn tại trong `PG_T24CORE.CUSTOMER` (bảng nguồn chỉ có 36,346 KH, đã che tên). Khoá băm vẫn khớp đúng; vấn đề là thiếu dữ liệu master.  |
| `recon_fct_excluded_balance`                             | 2 HĐ có số dư nhưng không có KH nên không vào được FCT (inner join DIM_CST theo thiết kế).                                                                                                                |
| `unique_key` INTF_LOAN_AR (CDR_DT, AR_CODE, GL_ITM_CODE) | SAOKE_CRB có 2 nhóm dòng cùng (APP_ID, SBV_CODE) nhưng khác `ASSET_TYPE`. INTF kế thừa grain của AR_BAL, mà spec không có cột ASSET_TYPE, nên 1 cặp dòng trùng khoá hiển thị (số tiền không bị nhân bản). |
| `dq_fct_dim_key_coverage` (mới)                          | 2 DIM key trong FCT rỗng 100%: `CCY_DIM_KEY` – `LD.CURRENCY` trên Oracle chỉ có giá trị ở 292/48,195 HĐ (kéo theo `EXG_RATE_TO_LCL_CCY` rỗng); `LMT_DIM_KEY` – bảng `LIMIT` nguồn chỉ có 2/6 cột.         |

Độ phủ DIM key của FCT_LOAN_AR (1,812 dòng): LOAN_AR/CST/OU/FNC_ST 100% · **PD 1,580 (87%)** · RSTC 278 · CCY 0 · LMT 0.

### 8.4 Số dòng theo tầng (`scripts/layer_stats.py 2025-10-07`)

Dung lượng trên máy local: `data/minio/bronze` 101 MB · `silver` 94 MB · `gold` 32 MB.

**PG_BRONZE (28 bảng, phân vùng ETL_DATE = 2025-10-07; số cột gồm 3 cột audit)**

| Bảng                                  | Số cột | Số dòng |
| ------------------------------------- | -----: | ------: |
| EBANKING_COL_UDF_VALUE                |      8 |  57,975 |
| FLEXBO_PGB_CONTRACT_UDF_MAP           |      7 |  57,336 |
| FLEXBO_PGB_LDTB_CONTRACT_MASTER       |      7 |  90,444 |
| FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE  |     22 |  90,010 |
| FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST    |      8 |       0 |
| FLEXBO_PGBLD_RT_CONTRACT_UDFIELD_HIST |      7 | 166,019 |
| LOS_APP_FACILITY                      |      6 | 464,431 |
| LOS_APP_LOAN_DISBURSEMENT             |      9 |      47 |
| LOS_APP_PRODUCT                       |      8 |     844 |
| SMY_STMT_DAILY                        |     60 |  18,066 |
| SOURCE_SAOKE_CRB                      |     21 | 101,904 |
| SOURCE_SAOKE_MVMT                     |      7 |     391 |
| T24CORE_COMPANY                       |      6 |     142 |
| T24CORE_CURRENCY                      |     10 |      35 |
| T24CORE_CURRENCY_HIS                  |      8 |   5,000 |
| T24CORE_CUSTOMER                      |     67 |  36,346 |
| T24CORE_DEPT_ACCT_OFFICER             |      6 |   2,119 |
| T24CORE_LD_LOANS_AND_DEPOSITS         |     43 |  48,195 |
| T24CORE_LD_LOANS_AND_DEPOSITS_HIS     |     33 |  32,529 |
| T24CORE_LD_SCHEDULE_DEFINE_MV         |      6 |  26,277 |
| T24CORE_LIMIT                         |     10 |  18,873 |
| T24CORE_MB_MG_SAVING_MULTI            |      7 |     127 |
| T24CORE_PD_PAYMENT_DUE                |     25 |   8,038 |
| T24CORE_PD_PAYMENT_DUE_HIS            |      7 | 100,000 |
| T24CORE_PD_PAYMENT_DUE_HIS_MV         |     24 |  56,159 |
| T24CORE_PD_PAYMENT_DUE_MV             |     27 |     267 |
| T24CORE_SECTOR                        |      6 |     117 |
| T24CORE_STMT_ENTRY                    |     49 |  89,869 |

**PG_SILVER (28 bảng; SCD: tổng dòng / dòng hiệu lực; snapshot/TXN: dòng của ngày chạy)**

| Bảng            | Số cột | Số dòng | Dòng hiệu lực |
| --------------- | -----: | ------: | ------------: |
| AR_AVY_SHD_ITM  |     19 |  58,873 |        58,873 |
| AR_BAL          |     17 |   2,480 |               |
| AR_DLQ_SMY      |     12 |   2,157 |               |
| AR_RATE_HIST    |     12 |  54,483 |        54,483 |
| AR_TVR_SMY      |     25 |   1,822 |               |
| AR_X_IP_RLTNP   |     16 |  54,483 |        54,483 |
| AST_AR_INT_SMY  |     11 |   1,822 |               |
| CCY             |      8 |      35 |            35 |
| CST             |     22 |  36,346 |        36,346 |
| CUST_INFO_DAILY |     65 |      25 |               |
| CV              |      8 |      12 |            12 |
| EMPE            |      6 |   2,119 |         2,119 |
| EXG_RATE        |      6 |      35 |               |
| IDV_EMP_ST      |      8 |  35,128 |        35,128 |
| IDV_PRFL        |     27 |  35,128 |        35,128 |
| IP_ALT_IDENTN   |     10 |       0 |               |
| IP_ELC_ADR      |      7 |   2,000 |         2,000 |
| IP_PST_ADR      |     10 |  72,692 |        72,692 |
| LMT             |     11 |  18,873 |        18,873 |
| LOAN_AR         |     31 |  54,483 |        54,483 |
| LOAN_AR_PRFL    |     37 |  54,483 |        54,483 |
| ORG_PRFL        |     30 |   1,217 |         1,217 |
| OU              |      6 |     142 |           142 |
| PD              |      7 |     241 |           241 |
| PST_ENTR        |     53 |      73 |               |
| SAOKE_GIAINGAN  |     75 |   2,480 |               |
| SAOKE_TINDUNG   |     69 |  48,195 |               |
| SK_LICH_TRA_NO  |    119 |     407 |               |

**PG_GOLD (13 bảng)**

| Bảng                 | Số cột | Số dòng | Dòng hiệu lực |
| -------------------- | -----: | ------: | ------------: |
| DIM_CCY              |      7 |      35 |            35 |
| DIM_CST              |      9 |  36,346 |        36,346 |
| DIM_FNC_ST           |      6 |       5 |             5 |
| DIM_LMT              |      6 |  18,873 |        18,873 |
| DIM_LOAN_AR          |     18 |  54,483 |        54,483 |
| DIM_OU               |      6 |     142 |           142 |
| DIM_PD               |      6 |     241 |           241 |
| DIM_RSTC_TP          |      6 |       7 |             7 |
| FCT_LOAN_AR          |     23 |   1,812 |               |
| INF_CST              |     65 |  36,345 |               |
| INTF_LOAN_AR         |     89 |   2,479 |               |
| INTF_LOAN_DSBR       |     25 |  54,483 |               |
| INTF_LOAN_REPYMT_SHD |     21 |  58,873 |               |

Ghi chú số liệu: `IP_ALT_IDENTN` = 0 dòng vì nguồn `PG_T24CORE.CUSTOMER` không có cột `NATIONAL_ID`;
`IP_ELC_ADR` chỉ có số di động (nguồn thiếu EMAIL/HOME_NUMBER); `PST_ENTR` = 73 bút toán thường (không phải `F%`) phát sinh ngày 2025-10-07.

## 9. Xử lý sự cố đã gặp

| Hiện tượng                                                                                                              | Nguyên nhân                                                                                                      | Khắc phục (đã áp dụng)                                                                                                                                                                                                                                                    |
| ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SCHEMA_NOT_FOUND lakehouse.default` khi mở session                                                                     | Thrift luôn `USE default`                                                                                        | entrypoint tạo namespace `default` bằng spark-sql trước khi bật Thrift                                                                                                                                                                                                    |
| Postgres `too many clients`                                                                                             | mỗi session Thrift tạo pool JDBC catalog riêng                                                                   | `spark.sql.hive.thriftServer.singleSession=true`                                                                                                                                                                                                                          |
| Spark driver OOM                                                                                                        | mỗi `ALTER COLUMN COMMENT` tạo 1 metadata.json + UI giữ lịch sử                                                  | chỉ ALTER khi comment đổi; `write.metadata.previous-versions-max=10`; giảm `spark.ui.retained*`                                                                                                                                                                           |
| MinIO 800% CPU, "Read timed out"                                                                                        | `mem_limit` 384 MB quá thấp với dữ liệu thật                                                                     | tăng 768 MB, `GOGC=50`                                                                                                                                                                                                                                                    |
| Git Bash đổi `/usr/app/...` thành `C:/Program Files/Git/...`                                                            | MSYS path conversion                                                                                             | `MSYS_NO_PATHCONV=1`                                                                                                                                                                                                                                                      |
| Driver OOM khi MERGE SCD2 (kể cả bảng 1.217 dòng), `SELECT` từng nhánh chạy được nhưng `UNION ALL` thì OOM              | Catalyst _constraint propagation_ bùng nổ tổ hợp với nhiều cột alias từ biểu thức CASE/regexp (macro `t24_date`) | `spark.sql.constraintPropagation.enabled=false` (truy vấn từ OOM → 8,7 s)                                                                                                                                                                                                 |
| OOM khi MERGE LOAN_AR (3 nhánh × ~10 JOIN)                                                                              | macro SCD2 tham chiếu `src` 2 lần → kế hoạch MERGE nhân đôi                                                      | tách bảng staging `pg_silver_stg.stg_*`; dbt `threads: 2`; `autoBroadcastJoinThreshold=4MB`, `memory.fraction=0.5`                                                                                                                                                        |
| `''` trong option JDBC báo lỗi cú pháp                                                                                  | literal Spark SQL dùng `'` chứ không phải `''`                                                                   | escape `'` trong `load_source.py`                                                                                                                                                                                                                                         |
| Lọc `COMMIT_ACTION <> 'D'` loại hết bút toán                                                                            | cột NULL trên dữ liệu thật                                                                                       | `NVL(COMMIT_ACTION,'_') <> 'D'`                                                                                                                                                                                                                                           |
| FCT dư nợ thấp bất thường                                                                                               | ép ngầm chuỗi số tiền sang INT bị tràn                                                                           | `CAST(... AS DECIMAL(25,5))` tường minh                                                                                                                                                                                                                                   |
| `UCP-0: Unable to start the Universal Connection Pool` (Trino catalog `oracle`)                                         | VPN chưa kết nối                                                                                                 | bật VPN; catalog `lakehouse` không phụ thuộc Oracle                                                                                                                                                                                                                       |
| Trino không khởi động: `Unrecognized VM option`                                                                         | sai tên cờ JVM trong `jvm.config`                                                                                | dùng `-XX:-OmitStackTraceInFastThrow`                                                                                                                                                                                                                                     |
| Unpause DAG → Airflow tự tạo 1 run theo lịch với `ds` = hôm nay (2026-11-03), chạy song song chờ slot của run thủ công  | `catchup=False` vẫn tạo run cho khoảng lịch gần nhất                                                             | đánh dấu run đó failed/skip, xoá phân vùng Bronze lỡ ghi (`DELETE … WHERE etl_date <> DATE '2025-10-07'` bằng Trino). Đã sửa gốc: ngày mặc định lấy từ Airflow Variable `etl_date` (= 2025-10-07) thay vì `ds`, nên run theo lịch cũng ghi đúng ngày nghiệp vụ của Oracle |
| Docker treo / API 500, Spark bị restart sau `dbt test`                                                                  | Spark + Trino + CloudBeaver + Airflow vượt 4 GB của WSL                                                          | tắt `trino cloudbeaver` khi chạy pipeline; hoặc tăng `memory=6GB` trong `.wslconfig`                                                                                                                                                                                      |
| Lần chạy đầu trên lakehouse rỗng: `duplicate key … iceberg_namespace_properties_pkey` (2 task Bronze, tự qua khi retry) | nhiều task song song cùng `CREATE SCHEMA pg_bronze`                                                              | entrypoint Spark tạo sẵn namespace `pg_bronze/pg_silver/pg_silver_stg/pg_gold` khi khởi động                                                                                                                                                                              |
