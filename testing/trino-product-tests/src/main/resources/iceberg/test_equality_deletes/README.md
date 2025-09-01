1. Create a base table with
   `io.trino.plugin.iceberg.TestIcebergV3.testEqualityDeleteAndDeletionVector`
2. Rewrite the absolute path in metadata files:
  - Get schema out of avro file: `avro-tools getschema avro-file > schema.avsc`
  - Get json out of avro file: `avro-tools tojson avro-file > file.json`
  - Update paths to reflect new HDFS path in json file like
    `local:///tpch/test_equality_deletes{uuid}` to
    `hdfs://hadoop-master:9000/user/hive/warehouse/default/test_equality_deletes`
  - Create updated avro file:
    `avro-tools fromjson --schema-file schema.avsc file.json > avro-file`
