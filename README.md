# Свободное исследование данных (ITMO 2025)

Разведочный анализ транзакций и гипотезы ценности для организации-заказчика. SQL на DuckDB работает **напрямую по Parquet**, без ETL.

## Данные
- `transaction_fraud_data.parquet` — транзакции: сумма, время, география, мерчант/категория (в т.ч. MCC), атрибуты платежа, риск-флаги и метка `is_fraud`.
- `historical_currency_exchange.parquet` — курсы валют по датам относительно USD (для нормализации сумм при мультивалютности).  
> Файлы данных **не коммитятся** (см. `.gitignore`). SQL читает их напрямую через `read_parquet(...)`.

### Ключевые поля (ожидаемые)
Идентификаторы: `transaction_id`, `customer_id`  
Время: `transaction_datetime` (или `timestamp`)  
Гео/мерчант: `country`, `city`, `vendor`, `merchant_category`/`vendor_category`, `mcc` (если есть)  
Платёжные: `amount`, `currency`, `card_type`, `is_card_present`, `channel`, `device`  
Риск/фрод: `is_outside_home_country`, `is_high_risk_vendor`, `is_weekend`, `is_fraud`  
Активность за последний час (если есть): `last_hour_activity.{num_transactions,total_amount,unique_merchants,unique_countries,max_single_amount}`

## Как запустить локально
```bash
python3 -m pip install -r requirements.txt
duckdb -c "DESCRIBE SELECT * FROM read_parquet('transaction_fraud_data.parquet') LIMIT 0;"
duckdb -f sql/eda.sql | tee REPORT.md
