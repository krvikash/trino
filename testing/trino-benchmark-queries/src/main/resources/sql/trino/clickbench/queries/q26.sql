SELECT SearchPhrase FROM ${database}.${schema}.hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
