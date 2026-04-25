SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM ${database}.${schema}.hits GROUP BY WatchID, ClientIP ORDER BY c, WatchID, ClientIP DESC LIMIT 10;
