-- sum(decimal(38,2)): char+bigint partition, bigint order (unique for ROWS), ROWS + RANGE frames over a precision above 28, large dataset via join
SELECT ca_state, ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_net_paid,
       SUM(CAST(ss_net_paid AS decimal(38, 2))) OVER (
           PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk, ss_ticket_number
           ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS sum_rows,
       SUM(CAST(ss_net_paid AS decimal(38, 2))) OVER (
           PARTITION BY ca_state, ss_item_sk ORDER BY ss_sold_date_sk
           RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS sum_range
FROM ${database}.${schema}.store_sales
JOIN ${database}.${schema}.customer_address ON ss_addr_sk = ca_address_sk
JOIN ${database}.${schema}.date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2002
ORDER BY ca_state, ss_item_sk, ss_sold_date_sk, ss_ticket_number
LIMIT 100;
