-- avg(decimal): multi bigint partition, no order (whole partition frame), scan-only
SELECT sr_store_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_amt,
       AVG(sr_return_amt) OVER (PARTITION BY sr_store_sk, sr_item_sk) AS avg_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_item_sk, sr_ticket_number
LIMIT 100;
