
This doc is intended to provide specific code points that one should verify for any changes while bumping JDBC
drivers.

- `com.starburstdata.trino.plugin.snowflake.parallel.StarburstResultStreamProvider` - Fetches signed chunk URL
    using logic from `net.snowflake.client.jdbc.DefaultResultStreamProvider`.
- `com.starburstdata.trino.plugin.snowflake.parallel.SnowflakeArrowPageSource#decodeArrowInputStream` - Reads
    arrow batches similar to `net.snowflake.client.jdbc.ArrowResultChunk#readArrowStream`.
- `com.starburstdata.trino.plugin.snowflake.parallel.writer.ConverterFactory.createSnowflakeConverter` -
    Maps Arrow types to Snowflake converter which converts to Java native types. 
- `com.starburstdata.trino.plugin.snowflake.parallel.ChunkParser.parseChunks` - Parses JSON response from 
   Snowflake about each chunk file similar to `net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1.parseChunkFiles`. 
