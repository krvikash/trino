-- avg(decimal): multi bigint partition, descending bigint order, RANGE BETWEEN 7 PRECEDING AND 7 FOLLOWING, scan-only
SELECT sr_store_sk, sr_returned_date_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       AVG(sr_return_amt) OVER (
           PARTITION BY sr_store_sk, sr_item_sk ORDER BY sr_returned_date_sk DESC
           RANGE BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS avg_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_item_sk, sr_returned_date_sk DESC, sr_ticket_number
LIMIT 100;
