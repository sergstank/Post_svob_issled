PRAGMA disable_progress_bar;

-- 0) Размер и схема
SELECT 'row_count' AS metric, COUNT(*)::BIGINT AS value FROM read_parquet('transaction_fraud_data.parquet');
DESCRIBE SELECT * FROM read_parquet('transaction_fraud_data.parquet') LIMIT 0;

-- 1) Доля фрода (общая)
SELECT 'fraud_share' AS metric, AVG(CAST(is_fraud AS DOUBLE)) AS value FROM read_parquet('transaction_fraud_data.parquet');

-- 2) Доля фрода у high-risk
SELECT 'high_risk_fraud_share' AS metric, AVG(CAST(is_fraud AS DOUBLE)) AS value FROM read_parquet('transaction_fraud_data.parquet') WHERE COALESCE(CAST(is_high_risk_vendor AS BOOLEAN), FALSE)=TRUE;

-- 3) Среднее число транзакций на клиента-час
WITH base AS (
  SELECT customer_id AS id,
         date_trunc('hour', CAST(timestamp AS TIMESTAMP)) AS h
  FROM read_parquet('transaction_fraud_data.parquet')
),
per_ch AS (
  SELECT id, h, COUNT(*) AS cnt FROM base WHERE h IS NOT NULL GROUP BY 1,2
)
SELECT 'tx_per_client_hour_avg' AS metric, AVG(cnt) AS value FROM per_ch;

-- 4) Топ стран по числу мошеннических операций
SELECT country, COUNT(*) AS fraud_n FROM read_parquet('transaction_fraud_data.parquet') WHERE CAST(is_fraud AS INTEGER)=1 AND country IS NOT NULL GROUP BY 1 ORDER BY fraud_n DESC NULLS LAST LIMIT 10;

-- 5) Города: средний чек (без плейсхолдеров)
WITH cities AS (
  SELECT city, AVG(CAST(amount AS DOUBLE)) AS avg_amount, COUNT(*) AS n
  FROM read_parquet('transaction_fraud_data.parquet')
  WHERE city IS NOT NULL AND amount IS NOT NULL
    AND lower(CAST(city AS VARCHAR)) NOT IN ('unknown city','unknown','n/a','na','null','none','')
  GROUP BY 1
)
SELECT * FROM cities ORDER BY avg_amount DESC LIMIT 10;

-- 6) Fast-food по MCC=5814 (раскомментируй, если есть столбец mcc)
-- SELECT city, AVG(CAST(amount AS DOUBLE)) AS avg_amount, COUNT(*) AS n
-- FROM read_parquet('transaction_fraud_data.parquet') WHERE TRY_CAST(mcc AS INTEGER)=5814 AND city IS NOT NULL AND amount IS NOT NULL GROUP BY 1 ORDER BY avg_amount DESC NULLS LAST LIMIT 10;
-- 0b) Временной охват
SELECT MIN(timestamp) AS ts_min, MAX(timestamp) AS ts_max
FROM read_parquet('transaction_fraud_data.parquet');
-- 0b) Временной охват
SELECT MIN(timestamp) AS ts_min, MAX(timestamp) AS ts_max
FROM read_parquet('transaction_fraud_data.parquet');
-- 4b) По часам суток
SELECT EXTRACT(hour FROM timestamp) AS hour_of_day,
       COUNT(*) AS n,
       AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1 ORDER BY 1;
-- 8b) Выходные × Вне домашней страны
SELECT is_weekend,
       is_outside_home_country,
       COUNT(*) AS n,
       AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1,2
ORDER BY n DESC;
-- 9b) Категории мерчантов (топ по доле при n>=1000)
WITH agg AS (
  SELECT vendor_category,
         COUNT(*) AS n,
         AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
  FROM read_parquet('transaction_fraud_data.parquet')
  GROUP BY 1
)
SELECT * FROM agg
WHERE n >= 1000
ORDER BY fraud_share DESC NULLS LAST
LIMIT 15;
-- 10) Активность за последний час vs фрод
SELECT is_fraud,
       AVG(CAST(last_hour_activity.num_transactions AS DOUBLE)) AS avg_tx_1h,
       AVG(CAST(last_hour_activity.total_amount     AS DOUBLE)) AS avg_amt_1h
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1;
