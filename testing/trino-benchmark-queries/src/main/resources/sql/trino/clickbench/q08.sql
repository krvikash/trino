SELECT AdvEngineID, COUNT(*) FROM ${database}.${schema}.hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
