-- row_number: single bigint partition, decimal+bigint order, scan-only
SELECT sr_store_sk, sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_amt,
       ROW_NUMBER() OVER (PARTITION BY sr_store_sk ORDER BY sr_return_amt DESC, sr_item_sk, sr_ticket_number) AS rn
FROM ${database}.${schema}.store_returns
ORDER BY sr_store_sk, rn
LIMIT 100;
