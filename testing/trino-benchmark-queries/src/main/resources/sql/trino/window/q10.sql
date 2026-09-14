-- row_number: multi bigint partition, multi decimal+bigint order, scan-only
SELECT sr_store_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_amt, sr_net_loss,
       ROW_NUMBER() OVER (PARTITION BY sr_store_sk, sr_item_sk ORDER BY sr_return_amt DESC, sr_net_loss DESC, sr_ticket_number) AS rn
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, sr_item_sk, rn
LIMIT 100;
