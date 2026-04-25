SELECT 1, URL, COUNT(*) AS c FROM ${database}.${schema}.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
