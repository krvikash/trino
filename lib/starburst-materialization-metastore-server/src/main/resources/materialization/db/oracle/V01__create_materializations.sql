CREATE TABLE materializations (
    metastore_id               VARCHAR(128)  NOT NULL,
    source_catalog             VARCHAR(256)  NOT NULL,
    source_schema              VARCHAR(256)  NOT NULL,
    source_table               VARCHAR(256)  NOT NULL,
    storage_table_catalog      VARCHAR(256)  NOT NULL,
    storage_table_schema       VARCHAR(256)  NOT NULL,
    storage_table_name         VARCHAR(256)  NOT NULL,
    storage_table_unique_id    VARCHAR(1024) NOT NULL,
    last_known_fresh_time      TIMESTAMP(6)  NOT NULL,
    grace_period_millis        NUMBER(19),
    ir_versions                CLOB          NOT NULL,
    catalog_ir_versions        CLOB          NOT NULL,
    computation_plan_root      CLOB          NOT NULL,
    created_at                 TIMESTAMP(6)  NOT NULL,
    last_modified_at           TIMESTAMP(6)  NOT NULL,
    PRIMARY KEY (metastore_id, source_catalog, source_schema, source_table)
)
