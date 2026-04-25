SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM ${database}.${schema}.hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
