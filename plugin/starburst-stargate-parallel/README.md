# Starburst Enterprise Stargate Parallel Connector

A parallel version of the Stargate connector, which allows executing queries on
other Starburst Enterprise, or Galaxy clusters.

Example use case, where a cluster running in AWS can query data from a different
geographical location, secured by a remote cluster.

```mermaid
flowchart LR
    client[Client]
    subgraph Cloud
        s3[(S3)]
        local_iceberg[Iceberg catalog A]
        local_stargate_iceberg[Iceberg catalog B]
        local_stargate_pgsql[PostgreSQL catalog C]
        subgraph Local cluster
            local_iceberg
            local_stargate_iceberg
            local_stargate_pgsql
        end
    end
    subgraph On-prem 
        hdfs[(HDFS)]
        pgsql[(PostgreSQL)]
        remote_iceberg[Iceberg catalog D]
        remote_pgsql[PostgreSQL catalog E]
        subgraph Remote cluster
            remote_iceberg
            remote_pgsql
        end
    end
    client-->local_iceberg
    client-->local_stargate_iceberg
    client-->local_stargate_pgsql
    local_iceberg-->s3
    local_stargate_iceberg-->remote_iceberg
    remote_iceberg-->hdfs
    local_stargate_pgsql-->remote_pgsql
    remote_pgsql-->pgsql
```

```mermaid
sequenceDiagram
    autonumber

    actor client as Client
    box Local cluster
    participant clocal as Local coordinator
    participant wlocal as Local workers
    end
    box Cloud
    participant s3 as Spooling storage (ex. S3)
    end
    box Remote cluster
    participant cremote as Remote coordinator
    participant wremote as Remote workers
    participant hdfs as Object store (ex. HDFS)
    end

    client ->> clocal: Query

    clocal ->> cremote: Query tables
    Note right of clocal: One or more,<br/>executed in parallel
    par Parallelized across remote workers
        cremote ->> wremote: Generate splits
        wremote ->> hdfs: Get data
        hdfs -->> wremote: Return data
        wremote -->> s3: Put segments
        wremote -->> cremote: Segment URL
    end
    cremote ->> clocal: Segment URLs

        par Parallelized across local workers
            clocal ->> wlocal: Generate splits
            wlocal ->> s3: Get segment
            s3 -->> wlocal: Segment
            wlocal -->> clocal: Results
        end


    clocal -->> client: Results
```
