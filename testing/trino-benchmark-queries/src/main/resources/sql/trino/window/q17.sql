-- avg(decimal): multi bigint partition, unique decimal+bigint order, ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING, scan-only
SELECT sr_store_sk, sr_item_sk, sr_ticket_number, sr_return_amt,
       AVG(sr_return_amt) OVER (
           PARTITION BY sr_store_sk, sr_item_sk ORDER BY sr_return_amt DESC, sr_ticket_number
           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS avg_amt
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_item_sk, sr_return_amt DESC, sr_ticket_number
LIMIT 100;
