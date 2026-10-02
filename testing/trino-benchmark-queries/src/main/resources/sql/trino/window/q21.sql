-- avg(decimal): single bigint partition, bigint order, GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING (empty for the first peer group), scan-only
SELECT sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       AVG(sr_return_amt) OVER (
           PARTITION BY sr_store_sk ORDER BY sr_returned_date_sk
           GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS avg_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number
LIMIT 100;
