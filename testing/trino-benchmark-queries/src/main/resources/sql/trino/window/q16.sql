-- avg(decimal): single bigint partition, unique bigint order, ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING, scan-only
SELECT sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       AVG(sr_return_amt) OVER (
           PARTITION BY sr_store_sk ORDER BY sr_returned_date_sk, sr_ticket_number, sr_item_sk
           ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS avg_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_returned_date_sk, sr_ticket_number, sr_item_sk
LIMIT 100;
