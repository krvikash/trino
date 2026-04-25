SELECT URL, COUNT(*) AS c FROM ${database}.${schema}.hits GROUP BY URL ORDER BY c DESC LIMIT 10;
