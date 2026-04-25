SELECT UserID, COUNT(*) FROM ${database}.${schema}.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
