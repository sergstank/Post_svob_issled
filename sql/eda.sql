
---

# sql/eda.sql (полезные запросы для отчёта)
Скопируй целиком:

```sql
PRAGMA disable_progress_bar;

-- 0) Размер и схема
SELECT 'row_count' AS metric, COUNT(*)::BIGINT AS value
FROM read_parquet('transaction_fraud_data.parquet');

DESCRIBE SELECT * FROM read_parquet('transaction_fraud_data.parquet') 
LIMIT 0;

-- 1) Доля фрода (общая)
SELECT 'fraud_share' AS metric,
       AVG(CAST(is_fraud AS DOUBLE)) AS value
FROM read_parquet('transaction_fraud_data.parquet');

-- 2) Доля фрода у high-risk продавцов
SELECT 'high_risk_fraud_share' AS metric,
       AVG(CAST(is_fraud AS DOUBLE)) AS value
FROM read_parquet('transaction_fraud_data.parquet')
WHERE COALESCE(CAST(is_high_risk_vendor AS BOOLEAN), FALSE)=TRUE;

-- 3) Среднее число транзакций на клиента в час
WITH base AS (
  SELECT customer_id AS id,
         date_trunc('hour', CAST(transaction_datetime AS TIMESTAMP)) AS h
  FROM read_parquet('transaction_fraud_data.parquet')
),
per_ch AS (
  SELECT id, h, COUNT(*) AS cnt
  FROM base
  WHERE h IS NOT NULL
  GROUP BY 1,2
)
SELECT 'tx_per_client_hour_avg' AS metric, AVG(cnt) AS value FROM per_ch;

-- 4) Время суток / дни недели
SELECT EXTRACT(hour FROM CAST(transaction_datetime AS TIMESTAMP)) AS 
hour_of_day,
       COUNT(*) AS n,
       AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1 ORDER BY 1;

SELECT EXTRACT(dow FROM CAST(transaction_datetime AS TIMESTAMP)) AS dow,
       COUNT(*) AS n,
       AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1 ORDER BY 1;

-- 5) Топ стран по фроду
SELECT country, COUNT(*) AS fraud_n
FROM read_parquet('transaction_fraud_data.parquet')
WHERE CAST(is_fraud AS INTEGER)=1 AND country IS NOT NULL
GROUP BY 1 ORDER BY fraud_n DESC NULLS LAST LIMIT 10;

WITH agg AS (
  SELECT country,
         COUNT(*) AS n,
         AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
  FROM read_parquet('transaction_fraud_data.parquet')
  WHERE country IS NOT NULL
  GROUP BY 1
)
SELECT * FROM agg WHERE n >= 500 ORDER BY fraud_share DESC NULLS LAST 
LIMIT 10;

-- 6) Города: средний чек (без placeholder-города)
WITH cities AS (
  SELECT city, AVG(CAST(amount AS DOUBLE)) AS avg_amount, COUNT(*) AS n
  FROM read_parquet('transaction_fraud_data.parquet')
  WHERE city IS NOT NULL AND amount IS NOT NULL
    AND lower(CAST(city AS VARCHAR)) NOT IN ('unknown 
city','unknown','n/a','na','null','none','')
  GROUP BY 1
)
SELECT * FROM cities ORDER BY avg_amount DESC LIMIT 10;

-- 7) Fast-food: mcc=5814 (если поле mcc есть)
SELECT city,
       AVG(CAST(amount AS DOUBLE)) AS avg_amount,
       COUNT(*) AS n
FROM read_parquet('transaction_fraud_data.parquet')
WHERE TRY_CAST(mcc AS INTEGER)=5814
  AND city IS NOT NULL AND amount IS NOT NULL
GROUP BY 1
ORDER BY avg_amount DESC NULLS LAST
LIMIT 10;

-- 8) Каналы/устройства
SELECT channel, COUNT(*) AS n, AVG(CAST(is_fraud AS DOUBLE)) AS 
fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1 ORDER BY n DESC NULLS LAST LIMIT 10;

SELECT device, COUNT(*) AS n, AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1 ORDER BY n DESC NULLS LAST LIMIT 10;

-- 9) High-risk вклад
SELECT is_high_risk_vendor,
       COUNT(*) AS n,
       SUM(CASE WHEN CAST(is_fraud AS INTEGER)=1 THEN 1 ELSE 0 END) AS 
fraud_n,
       AVG(CAST(is_fraud AS DOUBLE)) AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet')
GROUP BY 1
ORDER BY fraud_n DESC NULLS LAST;

-- 10) (Опционально) Нормализация сумм по курсам
-- Пример заготовки: расплавить курсы в пары (date,currency,rate) и 
джоинить по (DATE(transaction_datetime), currency)
-- Реализуется под конкретную схему таблицы historical_currency_exchange.

