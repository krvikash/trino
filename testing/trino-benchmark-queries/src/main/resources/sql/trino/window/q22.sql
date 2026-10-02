-- avg(decimal): char+bigint partition, bigint order (unique for ROWS), default + ROWS + RANGE + GROUPS frames in one window, large dataset via join
SELECT ca_state, ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_net_paid,
       AVG(ss_net_paid) OVER (PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk) AS avg_default,
       AVG(ss_net_paid) OVER (
           PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk, ss_ticket_number
           ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS avg_rows,
       AVG(ss_net_paid) OVER (
           PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk
           RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_range,
       AVG(ss_net_paid) OVER (
           PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk
           GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg_groups
FROM ${database}.${schema}.store_sales
JOIN ${database}.${schema}.customer_address ON ss_addr_sk = ca_address_sk
JOIN ${database}.${schema}.date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2002
ORDER BY ca_state, ss_item_sk, ss_sold_date_sk, ss_ticket_number
LIMIT 100;
