SELECT SearchPhrase, COUNT(*) AS c FROM ${database}.${schema}.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
