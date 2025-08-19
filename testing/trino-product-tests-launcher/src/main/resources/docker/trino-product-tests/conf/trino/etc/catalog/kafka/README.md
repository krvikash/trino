# Kafka Product Test Schemas

This directory contains schema and table definition files for Kafka product tests.

## File Types

- All files ending with `.json` are treated as Kafka table definitions.
- These `.json` files should be referenced in the corresponding Kafka environment under `environment/multinode-kafka/` for product tests.

## Schema Formats

- The schemas cover three formats:
    1. **Avro** (`*avro*`)
    2. **Protobuf** (`*protobuf*`)
    3. **JSON** (`*json*`)
- The format can be identified by the file name (e.g., `read_basic_datatypes_protobuf.json` is for Protobuf).

## File Description

- Each `.json` file is a JSON representation of the `KafkaTopicDescription` class.
- For the file not ending with `.json`, it is a format described about the schema, i,e the schema that follows the confluent schema registry format.
- These files describe Kafka topics, including table name, schema, topic name, key, and message structure.

## Product Tests

- Product tests are located in `trino-product-tests/src/main/java/io/trino/tests/product/kafka/`.
- Tests use these schema files to validate Kafka integration.

## Adding New Schemas

1. Add your `.json` schema file to this directory.
2. Reference it in the appropriate Kafka environment under `environment/multinode-kafka/`.
3. Ensure the file follows the `KafkaTopicDescription` format and the table name is the file name created in step 1.