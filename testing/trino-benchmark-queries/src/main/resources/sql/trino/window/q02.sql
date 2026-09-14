-- rank: multi bigint partition, multi decimal order, scan-only
SELECT sr_store_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_amt, sr_net_loss,
       RANK() OVER (PARTITION BY sr_store_sk, sr_item_sk ORDER BY sr_return_amt DESC, sr_net_loss DESC) AS rnk
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_item_sk, rnk, sr_ticket_number
LIMIT 100;
