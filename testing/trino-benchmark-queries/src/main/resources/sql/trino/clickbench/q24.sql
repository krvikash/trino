SELECT * FROM ${database}.${schema}.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
