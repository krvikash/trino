-- sum(decimal): single bigint partition, bigint order, default frame (RANGE UNBOUNDED PRECEDING to CURRENT ROW), scan-only
SELECT sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       SUM(sr_return_amt) OVER (PARTITION BY sr_store_sk ORDER BY sr_returned_date_sk) AS sum_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number
LIMIT 100;
