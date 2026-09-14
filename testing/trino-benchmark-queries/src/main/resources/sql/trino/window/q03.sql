-- rank: single char partition, single decimal order, large dataset via join
SELECT ca_state, ss_sold_date_sk, ss_store_sk, ss_item_sk, ss_customer_sk, ss_ticket_number, ss_net_paid,
       RANK() OVER (PARTITION BY ca_state ORDER BY ss_net_paid DESC) AS rnk
FROM ${database}.${schema}.store_sales
JOIN ${database}.${schema}.customer_address ON ss_addr_sk = ca_address_sk
JOIN ${database}.${schema}.date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2002
ORDER BY ca_state, rnk, ss_sold_date_sk, ss_item_sk, ss_ticket_number
LIMIT 100;
