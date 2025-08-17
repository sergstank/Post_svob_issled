-- Схема таблицы
DESCRIBE SELECT * FROM read_parquet('transaction_fraud_data.parquet') LIMIT 0;

-- Доля фрода (пример; при необходимости поправь имя столбца)
SELECT ceil(avg(CAST(is_fraud AS DOUBLE))*10)/10 AS fraud_share
FROM read_parquet('transaction_fraud_data.parquet');
