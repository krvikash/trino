-- avg(decimal): multi char+varchar partition, unique bigint order, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, large dataset via join
SELECT ca_state, ca_county, ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_net_paid,
       AVG(ss_net_paid) OVER (
           PARTITION BY ca_state, ca_county ORDER BY ss_sold_date_sk, ss_ticket_number, ss_item_sk
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_paid
FROM ${database}.${schema}.store_sales
JOIN ${database}.${schema}.customer_address ON ss_addr_sk = ca_address_sk
JOIN ${database}.${schema}.date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2002
ORDER BY ca_state, ca_county, ss_sold_date_sk, ss_ticket_number, ss_item_sk
LIMIT 100;
