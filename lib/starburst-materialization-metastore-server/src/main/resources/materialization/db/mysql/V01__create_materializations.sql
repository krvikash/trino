CREATE TABLE materializations (
    metastore_id               VARCHAR(128)  NOT NULL,
    source_catalog             VARCHAR(128)  NOT NULL, -- smaller column size than other dbs so the utf8mb4 PK fits the InnoDB 3072-byte index-key limit: (128+128+256+256)*4 = 3072 bytes (the limit counts declared key-part bytes, not VARCHAR length prefixes)
    source_schema              VARCHAR(256)  NOT NULL,
    source_table               VARCHAR(256)  NOT NULL,
    storage_table_catalog      VARCHAR(256)  NOT NULL,
    storage_table_schema       VARCHAR(256)  NOT NULL,
    storage_table_name         VARCHAR(256)  NOT NULL,
    storage_table_unique_id    VARCHAR(1024) NOT NULL,
    last_known_fresh_time      TIMESTAMP(6)  NOT NULL,
    grace_period_millis        BIGINT,
    ir_versions                LONGTEXT      NOT NULL,
    catalog_ir_versions        LONGTEXT      NOT NULL,
    computation_plan_root      LONGTEXT      NOT NULL,
    created_at                 TIMESTAMP(6)  NOT NULL,
    last_modified_at           TIMESTAMP(6)  NOT NULL,
    PRIMARY KEY (metastore_id, source_catalog, source_schema, source_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
