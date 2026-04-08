SELECT SearchPhrase FROM ${database}.${schema}.hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
