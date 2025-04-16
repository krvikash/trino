# Starburst Snowflake Connector

This is a Starburst-developed version of Snowflake connector, independent of trino-snowflake.

## Parallel connector

```mermaid
---
config:
  theme: base
---

sequenceDiagram
    actor User
    box Starburst
        participant Coordinator
        participant Workers
    end

    box Snowflake Systems
        participant Snowflake JDBC Driver
        participant Snowflake
    end

    User->>Coordinator: Submit query
    activate Coordinator
    Coordinator->>Snowflake JDBC Driver: Execute query using JDBC
    activate Snowflake JDBC Driver
    Snowflake JDBC Driver->>Snowflake: Execute query
    activate Snowflake
    Snowflake-->>Snowflake JDBC Driver: Return results
    deactivate Snowflake
    Snowflake JDBC Driver-->>Coordinator: Snowflake response with results and metadata
    deactivate Snowflake JDBC Driver
    deactivate Coordinator

    Note over Coordinator,Snowflake JDBC Driver: For large result sets as of now Snowflake<br/>returns a list of pre-signed URLs to SSE-C<br/>encrypted objects on S3 using a per-query key<br/>and additional encryption headers needed to<br/>access them. This can be changed in future by<br/>Snowflake and is out of Starburst's control.<br/>

    alt Large query results?
        activate Coordinator
        Coordinator->>Coordinator: Generate splits with list of URLs from<br/>the query result metadata
        Coordinator->>Workers: Schedule splits
        activate Workers
        deactivate Coordinator

        loop For each URL in split
            Workers->>Snowflake: Fetch data from URL
            activate Snowflake
        
            Snowflake-->>Workers: Return data
            deactivate Snowflake
        end

        Workers-->>Coordinator: Retreive data
        deactivate Workers
        activate Coordinator
        Coordinator-->>User: Return results
        deactivate Coordinator
    else Small query results?
        Snowflake JDBC Driver-->>Coordinator: Snowflake response with inline results and metadata
        activate Coordinator

        Coordinator->>Coordinator: Extract inline results from Snowflake response
        Coordinator-->>User: Return results
        deactivate Coordinator
    end
```

## Snowflake JDBC driver version bumps

With each driver version bump, there is a chance that Arrow handling has changed and parallel code in this module needs adjusting.
Consult [snowflake-driver-bump.md](snowflake-driver-bump.md) for reference.

## Trivia 

- `-Dnet.snowflake.jdbc.enableBouncyCastle=true` is required for snowflake-jdbc driver to be able to [decrypt stronger keys](https://github.com/snowflakedb/snowflake-jdbc/issues/1683#issuecomment-2034442119)
- `--add-opens=java.base/java.nio=ALL-UNNAMED` is required for snowflake-jdbc driver due to Arrow usage
