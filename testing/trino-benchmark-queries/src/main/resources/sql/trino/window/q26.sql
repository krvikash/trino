-- sum(decimal): single bigint partition, unique bigint order, ROWS BETWEEN 10 PRECEDING AND CURRENT ROW, scan-only
SELECT sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       SUM(sr_return_amt) OVER (
           PARTITION BY sr_store_sk ORDER BY sr_returned_date_sk, sr_ticket_number, sr_item_sk
           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS sum_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_returned_date_sk, sr_ticket_number, sr_item_sk
LIMIT 100;
