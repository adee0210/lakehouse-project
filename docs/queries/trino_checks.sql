-- =============================================================================
-- Các câu query kiểm tra nhanh trên Trino (mở CloudBeaver http://localhost:8978
-- -> kết nối "Trino - Lakehouse" -> SQL Editor -> dán & chạy từng câu bằng Ctrl+Enter)
-- ETL_DATE mặc định: 2025-10-07
-- =============================================================================

-- 1. Danh sách bảng theo tầng
SHOW TABLES FROM lakehouse.pg_bronze;
SHOW TABLES FROM lakehouse.pg_silver;
SHOW TABLES FROM lakehouse.pg_gold;

-- 2. Lịch sử ghi (snapshot Iceberg) của 1 bảng – xem dbt đã chạy lúc nào, ghi bao nhiêu dòng
SELECT committed_at, operation, element_at(summary, 'added-records') AS added,
       element_at(summary, 'deleted-records') AS deleted, element_at(summary, 'total-records') AS total
FROM lakehouse.pg_gold."fct_loan_ar$snapshots" ORDER BY committed_at DESC;

-- 3. Bronze vs nguồn Oracle (cần VPN) – số dòng phải bằng nhau
SELECT 'oracle' AS side, count(*) FROM oracle.pg_t24core.ld
UNION ALL
SELECT 'bronze', count(*) FROM lakehouse.pg_bronze.t24core_ld WHERE etl_date = DATE '2025-10-07';

-- 4. SCD2: mỗi AR_ID chỉ có đúng 1 bản ghi hiện hành (kết quả phải rỗng)
SELECT ar_id, count(*) FROM lakehouse.pg_silver.loan_ar WHERE sys_exp_dt IS NULL GROUP BY ar_id HAVING count(*) > 1;

-- 5. Đối soát dư nợ: Silver AR_BAL = Gold INTF_LOAN_AR (= 1.072.809.438.195); FCT thấp hơn 2 HĐ không có KH
SELECT 'AR_BAL (silver)' AS layer,
       sum(CASE WHEN gl_itm_code LIKE '2%' AND cls_bal_lcy < 0 THEN abs(cls_bal_lcy) ELSE 0 END) AS du_no
FROM lakehouse.pg_silver.ar_bal WHERE cdr_dt = DATE '2025-10-07'
UNION ALL
SELECT 'INTF_LOAN_AR (gold)', sum(amt_lcy) FROM lakehouse.pg_gold.intf_loan_ar WHERE cdr_dt = DATE '2025-10-07'
UNION ALL
SELECT 'FCT_LOAN_AR (gold)', sum(otsnd_amt_lcy) FROM lakehouse.pg_gold.fct_loan_ar;

-- 6. Star schema: FCT join DIM (bản ghi hiện hành) – dư nợ theo sản phẩm & chi nhánh
SELECT p.pd_code, o.ou_code, o.ou_nm, count(*) AS so_hd, sum(f.otsnd_amt_lcy) AS du_no
FROM lakehouse.pg_gold.fct_loan_ar f
JOIN lakehouse.pg_gold.dim_pd p ON f.pd_dim_key = p.dim_key AND p.sys_exp_dt IS NULL
JOIN lakehouse.pg_gold.dim_ou o ON f.ou_dim_key = o.dim_key AND o.sys_exp_dt IS NULL
GROUP BY 1, 2, 3 ORDER BY du_no DESC LIMIT 20;

-- 7. Interface khách hàng
SELECT * FROM lakehouse.pg_gold.inf_cst LIMIT 50;
