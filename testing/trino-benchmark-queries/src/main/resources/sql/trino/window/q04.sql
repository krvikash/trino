-- rank: multi char+varchar partition, varchar+decimal order, large dataset via join
SELECT ca_state, ca_county, ss_sold_date_sk, ss_store_sk, ss_item_sk, ss_customer_sk, ss_ticket_number, ss_net_paid,
       RANK() OVER (PARTITION BY ca_state, ca_county ORDER BY ca_city, ss_net_paid DESC) AS rnk
FROM ${database}.${schema}.store_sales
JOIN ${database}.${schema}.customer_address ON ss_addr_sk = ca_address_sk
JOIN ${database}.${schema}.date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2002
ORDER BY ca_state, ca_county, rnk, ss_sold_date_sk, ss_item_sk, ss_ticket_number
LIMIT 100;
