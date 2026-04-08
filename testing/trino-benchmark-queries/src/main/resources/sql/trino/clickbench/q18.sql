SELECT UserID, SearchPhrase, COUNT(*) FROM ${database}.${schema}.hits GROUP BY UserID, SearchPhrase ORDER BY UserID, SearchPhrase LIMIT 10;
