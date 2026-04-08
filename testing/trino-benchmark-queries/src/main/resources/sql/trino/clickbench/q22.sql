SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM ${database}.${schema}.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c, SearchPhrase DESC LIMIT 10;
