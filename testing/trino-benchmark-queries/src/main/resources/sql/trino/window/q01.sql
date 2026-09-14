-- rank: single bigint partition, single decimal order, scan-only
SELECT sr_store_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_amt,
       RANK() OVER (PARTITION BY sr_store_sk ORDER BY sr_return_amt DESC) AS rnk
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, rnk, sr_item_sk, sr_ticket_number
LIMIT 100;
