# Unity metastore Delta smoke tests

These smoke tests validate Delta connector behavior against a live Databricks Unity Catalog
workspace over a Hive Thrift SQL endpoint. Most tables are created/dropped per-test via 
`onDatabricks().execute(...)`, but some fixtures are provisioned once and reused across runs.

## Permanent fixtures

Schema `main.tpch_delta_ci_external` (created if absent by `createTpchTables`) holds:

- `nation`, `region` — TPCH tables re-created from `SAMPLES.TPCH` when row counts drift.
- `hive_table` — Parquet CTAS from `SAMPLES.TPCH.nation`.
- `mv_nation_external_metadata` — materialized view with `pipelines.externalMetadata.enabled=true`.
- `mv_nation_without_external_metadata` — materialized view without the above flag.

### Provisioning the materialized views

`CREATE MATERIALIZED VIEW` requires a **DBSQL Serverless or Pro warehouse**, but the
classic Hive Thrift endpoint used by the tests rejects the statement with
`MATERIALIZED_VIEW_OPERATION_NOT_ALLOWED.REQUIRES_DBSQL_PRO_PLUS`. Run these once
in each Databricks workspace:


```sql
USE CATALOG main;
USE SCHEMA tpch_delta_ci_external;

CREATE MATERIALIZED VIEW mv_nation_external_metadata
TBLPROPERTIES ('pipelines.externalMetadata.enabled' = 'true')
AS SELECT nationkey, name FROM nation WHERE nationkey < 3;

CREATE MATERIALIZED VIEW mv_nation_without_external_metadata
AS SELECT nationkey FROM nation WHERE nationkey < 3;
```

Rows expected:
`(0, 'ALGERIA'), (1, 'ARGENTINA'), (2, 'BRAZIL')`.
