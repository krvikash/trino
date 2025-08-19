/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.trino.tempto.ProductTest;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.kafka.KafkaMessage;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.trino.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.trino.tempto.query.QueryResult;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.KAFKA_CONFLUENT_LICENSE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.utils.SchemaRegistryClientUtils.getSchemaRegistryClient;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestKafkaJsonReadsSmokeTest
        extends ProductTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String KAFKA_SCHEMA = "product_tests";

    private static final KafkaCatalog KAFKA_CATALOG = new KafkaCatalog("kafka", "", true, new JsonMessageSerializer());
    private static final KafkaCatalog KAFKA_SCHEMA_REGISTRY_CATALOG = new KafkaCatalog("kafka_schema_registry", "_schema_registry", false, new SchemaRegistryJsonMessageSerializer());

    private static final String ALL_BASIC_DATATYPES_JSON_TOPIC_NAME = "read_all_basic_datatypes_json";
    // file table description supplier using suffix to verify if it is a table, add .schema to the file name to avoid
    // let the supplier think it is a table/topic
    private static final String ALL_BASIC_DATATYPE_SCHEMA_PATH = "/docker/trino-product-tests/conf/trino/etc/catalog/kafka/basic_datatypes_json.json.schema";
    private static final String ALL_NULL_JSON_TOPIC_NAME = "read_all_null_json";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testSelectPrimitiveDataType()
            throws Exception
    {
        selectPrimitiveDataType(KAFKA_CATALOG);
    }

    @Test(groups = {KAFKA_CONFLUENT_LICENSE, PROFILE_SPECIFIC_TESTS})
    public void testSelectPrimitiveDataTypeWithSchemaRegistry()
            throws Exception
    {
        selectPrimitiveDataType(KAFKA_SCHEMA_REGISTRY_CATALOG);
    }

    private static void selectPrimitiveDataType(KafkaCatalog kafkaCatalog)
            throws Exception
    {
        PrimitiveDataTypeRecord record = new PrimitiveDataTypeRecord(
                "foobar",
                127L,
                234.567,
                true,
                "2020-01-01T10:00:00.123Z",
                "2020-01-01",
                "10:00:00.123Z");
        String topicName = ALL_BASIC_DATATYPES_JSON_TOPIC_NAME + kafkaCatalog.topicNameSuffix();
        createJsonTable(ALL_BASIC_DATATYPE_SCHEMA_PATH, ALL_BASIC_DATATYPES_JSON_TOPIC_NAME, topicName, record, kafkaCatalog.messageSerializer());
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = onTrino().executeQuery(format("select a_varchar, a_bigint, a_double, a_boolean, to_iso8601(a_timestamp), CAST(a_date AS varchar), CAST(a_time AS varchar) from %s.%s", kafkaCatalog.catalogName(), KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(
                            "foobar",
                            127,
                            234.567,
                            true,
                            "2020-01-01T10:00:00.123Z",
                            "2020-01-01",
                            "10:00:00.123+00:00"));
                });
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testSelectNullType()
            throws Exception
    {
        selectNullType(KAFKA_CATALOG);
    }

    @Test(groups = {KAFKA_CONFLUENT_LICENSE, PROFILE_SPECIFIC_TESTS})
    public void testSelectNullTypeWithSchemaRegistry()
            throws Exception
    {
        selectNullType(KAFKA_SCHEMA_REGISTRY_CATALOG);
    }

    private static void selectNullType(KafkaCatalog kafkaCatalog)
            throws Exception
    {
        String topicName = ALL_NULL_JSON_TOPIC_NAME + kafkaCatalog.topicNameSuffix();
        createJsonTable(ALL_BASIC_DATATYPE_SCHEMA_PATH, ALL_NULL_JSON_TOPIC_NAME, topicName, new PrimitiveDataTypeRecord(null, null, null, null, null, null, null), kafkaCatalog.messageSerializer());
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = onTrino().executeQuery(format("select a_varchar, a_bigint, a_double, a_boolean, a_timestamp, a_date, a_time from %s.%s", kafkaCatalog.catalogName(), KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null));
                });
    }

    private record KafkaCatalog(String catalogName, String topicNameSuffix, boolean columnMappingSupported, MessageSerializer messageSerializer)
    {
        private KafkaCatalog
        {
            requireNonNull(catalogName, "catalogName is null");
            requireNonNull(topicNameSuffix, "topicNameSuffix is null");
            requireNonNull(messageSerializer, "messageSerializer is null");
        }
    }

    private static void createJsonTable(String schemaPath, String tableName, String topicName, Record record, MessageSerializer messageSerializer)
            throws Exception
    {
        String schema = Files.readString(new File(schemaPath).toPath());
        createJsonTable(new JsonSchema(schema), tableName, topicName, record, messageSerializer);
    }

    private static void createJsonTable(JsonSchema schema, String tableName, String topicName, Record record, MessageSerializer messageSerializer)
            throws Exception
    {
        byte[] jsonData = messageSerializer.serialize(topicName, schema, record);

        KafkaTableDefinition tableDefinition = new KafkaTableDefinition(
                KAFKA_SCHEMA + "." + tableName,
                topicName,
                new ListKafkaDataSource(ImmutableList.of(
                        new KafkaMessage(
                                contentsBuilder()
                                        .appendBytes(jsonData)
                                        .build()))),
                1,
                1);
        KafkaTableManager kafkaTableManager = (KafkaTableManager) testContext().getDependency(TableManager.class, "kafka");
        kafkaTableManager.createImmutable(tableDefinition, tableHandle(tableName).inSchema(KAFKA_SCHEMA));
    }

    @FunctionalInterface
    private interface MessageSerializer
    {
        byte[] serialize(String topic, JsonSchema parsedSchema, Record record)
                throws IOException;
    }

    private static final class JsonMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, JsonSchema jsonSchema, Record record)
                throws IOException
        {
            return MAPPER.writeValueAsBytes(record);
        }
    }

    private static final class SchemaRegistryJsonMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, JsonSchema jsonSchema, Record record)
                throws IOException
        {
            Schema schema = jsonSchema.rawSchema();
            checkArgument(schema instanceof ObjectSchema objectSchema && objectSchema.getPropertySchemas().keySet().containsAll(record.keys()),
                    "Expected an object schema contains record all columns, but got type: %s, schema: %s", schema.getClass().getSimpleName(), schema.toString());

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                // write magic byte
                out.write((byte) 0);

                // write schema id
                int schemaId = getSchemaRegistryClient().register(
                        topic + "-value",
                        jsonSchema);
                out.write(Ints.toByteArray(schemaId));

                // write data
                out.write(MAPPER.writeValueAsBytes(record));

                return out.toByteArray();
            }
            catch (RestClientException clientException) {
                throw new RuntimeException(clientException);
            }
        }
    }

    private record PrimitiveDataTypeRecord(
            @JsonProperty("a_varchar") String varchar,
            @JsonProperty("a_bigint") Long bigint,
            @JsonProperty("a_double") Double doubleValue,
            @JsonProperty("a_boolean") Boolean booleanValue,
            @JsonProperty("a_timestamp") String timestamp,
            @JsonProperty("a_date") String date,
            @JsonProperty("a_time") String time)
            implements Record
    {
        @Override
        public Set<String> keys()
        {
            return ImmutableSet.of("a_varchar", "a_bigint", "a_double", "a_boolean", "a_timestamp", "a_date", "a_time");
        }
    }

    private interface Record
    {
        Set<String> keys();
    }
}
