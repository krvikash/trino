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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.JsonMapperProvider;
import io.confluent.kafka.schemaregistry.annotations.Schema;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.trino.plugin.kafka.KafkaQueryRunner;
import io.trino.plugin.kafka.KafkaTopicFieldDescription;
import io.trino.plugin.kafka.schema.confluent.ubs.UbsMockSecurityAccrualCommand;
import io.trino.spi.type.Type;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
final class TestKafkaWithConfluentJsonSchemaRegistryMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private static final JsonMapper MAPPER = new JsonMapperProvider().get();
    private static final int MESSAGE_COUNT = 100;
    private static final ObjectSchema INITIAL_SCHEMA = ObjectSchema.builder()
            .addPropertySchema("col1", NumberSchema.builder().requiresInteger(true).build())
            .addPropertySchema("col2", StringSchema.builder().requiresString(true).build())
            .schemaOfAdditionalProperties(NumberSchema.builder().requiresNumber(true).build())
            .additionalProperties(true)
            .build();
    private static final ObjectSchema EVOLVED_SCHEMA = ObjectSchema.builder()
            .addPropertySchema("col1", NumberSchema.builder().requiresInteger(true).build())
            .addPropertySchema("col2", StringSchema.builder().requiresString(true).build())
            .addPropertySchema("col3", NumberSchema.builder().minimum(Double.MIN_VALUE).maximum(Double.MAX_VALUE).defaultValue(0.0).nullable(true).build())
            .schemaOfAdditionalProperties(NumberSchema.builder().requiresNumber(true).build())
            .additionalProperties(true)
            .build();

    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        testingKafka.start();
        return KafkaQueryRunner.builderForConfluentSchemaRegistry(testingKafka)
                .addConnectorProperties(ImmutableMap.of("kafka.confluent-subjects-cache-refresh-interval", "1ms"))
                .build();
    }

    @Test
    void testBasicTopic()
    {
        String topic = "topic-basic-MixedCase" + randomNameSuffix();
        assertTopic(
                topic,
                format("SELECT col1, col2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col1, col2, col3 FROM %s", toDoubleQuoted(topic)),
                false,
                properties());
    }

    @Test
    void testConstant()
    {
        String topic = "topic-constant-" + randomNameSuffix();
        List<ProducerRecord<String, ConstEvent>> messages = ImmutableList.of(new ProducerRecord<>(topic, "key-1", new ConstEvent("USA")));

        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topic);
        assertCount(topic, 1);

        assertThat(query("SELECT country FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (VARCHAR 'USA')");
    }

    @Test
    void testDateAndTimeTypes()
    {
        String topic = "topic-date-time-" + randomNameSuffix();
        assertNotExists(topic);

        List<ProducerRecord<String, DateTimeEvent>> messages = ImmutableList.of(new ProducerRecord<>(topic, "key-1", new DateTimeEvent("2025-08-23", "14:46:59.614Z")));

        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topic);
        assertCount(topic, 1);

        assertThat(query("SHOW COLUMNS FROM " + toDoubleQuoted(topic)))
                .skippingTypesCheck()
                .matches("VALUES ('%s-key', 'varchar', '', ''), ".formatted(topic.toLowerCase(ENGLISH)) +
                        "('date', 'date', '', ''), " +
                        "('time', 'time(3) with time zone', '', '')");

        assertThat(query("SELECT date, time FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (DATE '2025-08-23', TIME '14:46:59.614 +00:00')");
    }

    @Test
    void testTimestampTypes()
    {
        String topic = "topic-timestamp-" + randomNameSuffix();
        assertNotExists(topic);

        List<ProducerRecord<String, TimestampEvent>> messages = ImmutableList.of(
                new ProducerRecord<>(topic, "key-1", new TimestampEvent("2025-08-23T14:46:59.614Z")));

        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topic);
        assertCount(topic, 1);

        assertThat(query("SHOW COLUMNS FROM " + toDoubleQuoted(topic)))
                .skippingTypesCheck()
                .matches("VALUES ('%s-key', 'varchar', '', ''), ".formatted(topic.toLowerCase(ENGLISH)) +
                        "('timestamp', 'timestamp(3) with time zone', '', '')");

        assertThat(query("SELECT timestamp FROM " + toDoubleQuoted(topic)))
                .matches("VALUES TIMESTAMP '2025-08-23 14:46:59.614 UTC'");

        // invalid timestamp that are not following ISO 8601 format
        messages = ImmutableList.of(new ProducerRecord<>(topic, "key-2", new TimestampEvent("2023-10-01T10:10:10")));
        testingKafka.sendMessages(messages.stream(), properties());
        assertCount(topic, 2);
        assertThat(query("SELECT timestamp FROM " + toDoubleQuoted(topic) + " WHERE \"" + topic + "-key\" = 'key-2'"))
                .failure()
                .hasMessage("could not parse value '2023-10-01T10:10:10' as 'timestamp(3) with time zone' for column 'timestamp'");
    }

    @Test
    void testArrayTypes()
    {
        String topic = "topic-array-" + randomNameSuffix();
        assertNotExists(topic);

        List<ProducerRecord<String, ArrayEvent>> messages = ImmutableList.of(
                new ProducerRecord<>(
                        topic,
                        "key-1",
                        new ArrayEvent(
                                true,
                                ImmutableList.of(1, 2, 3),
                                ImmutableList.of("a", "b", "c"),
                                ImmutableList.of("2025-08-23T14:46:59.614080Z"),
                                ImmutableList.of("2025-08-23"),
                                ImmutableList.of("14:46:59.614Z"))));

        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topic);
        assertCount(topic, 1);

        assertThat(query("SHOW COLUMNS FROM " + toDoubleQuoted(topic)))
                .skippingTypesCheck()
                .matches("VALUES ('%s-key', 'varchar', '', ''), ".formatted(topic.toLowerCase(ENGLISH)) +
                        "('bool', 'boolean', '', ''), " +
                        "('arrtimestamp', 'array(timestamp(3) with time zone)', '', ''), " +
                        "('arrdate', 'array(date)', '', ''), " +
                        "('arrstring', 'array(varchar)', '', ''), " +
                        "('arrtime', 'array(time(3) with time zone)', '', ''), " +
                        "('arrint', 'array(bigint)', '', '')");

        assertThat(query("SELECT bool FROM " + toDoubleQuoted(topic)))
                .matches("VALUES true");
        assertThat(query("SELECT arrInt[1], arrInt[2], arrInt[3] FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (BIGINT '1', BIGINT '2', BIGINT '3')");
        assertThat(query("SELECT arrString[1], arrString[2], arrString[3] FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c')");
        assertThat(query("SELECT arrTimestamp[1], arrDate[1], arrTime[1] FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (TIMESTAMP '2025-08-23 14:46:59.614 UTC', DATE '2025-08-23', TIME '14:46:59.614 +00:00')");
    }

    @Test
    void testObjectType()
    {
        String topic = "topic-object-" + randomNameSuffix();
        assertNotExists(topic);

        List<ProducerRecord<String, ObjectEvent>> messages = ImmutableList.of(
                new ProducerRecord<>(topic, "key-1", new ObjectEvent("test-string", new NestedObjectEvent(
                        ImmutableList.of(new NestedObject(ImmutableList.of(1, 2, 3), "nested-array-string")), "nested-string"))));

        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topic);
        assertCount(topic, 1);

        assertThat(query("SHOW COLUMNS FROM " + toDoubleQuoted(topic)))
                .skippingTypesCheck()
                .matches("VALUES ('%s-key', 'varchar', '', ''), ".formatted(topic.toLowerCase(ENGLISH)) +
                        "('str', 'varchar', '', ''), " +
                        "('nestedobj', 'row(\"nestedString\" varchar, \"arrObject\" array(row(\"nestedArrayString\" varchar, \"arrInt\" array(bigint))))', '', '')");

        assertThat(query("SELECT nestedObj.arrObject[1].arrInt[1], nestedObj.arrObject[1].arrInt[2], nestedObj.arrObject[1].arrInt[3] FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (BIGINT '1', BIGINT '2', BIGINT '3')");
        assertThat(query("SELECT str, nestedObj.nestedString, nestedObj.arrObject[1].nestedArrayString FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (VARCHAR 'test-string', VARCHAR 'nested-string', VARCHAR 'nested-array-string')");
    }

    @Test
    void testReferenceType()
    {
        String referTopic = "topic-reference-" + randomNameSuffix();
        assertNotExists(referTopic);
        List<ProducerRecord<String, ReferenceEvent>> referenceMessages = ImmutableList.of(
                new ProducerRecord<>(referTopic, "key-1", new ReferenceEvent("reference-string", new ObjectEvent(
                        "test-string",
                        new NestedObjectEvent(ImmutableList.of(new NestedObject(ImmutableList.of(1, 2, 3), "nested-array-string")), "nested-string")))));
        testingKafka.sendMessages(referenceMessages.stream(), properties());
        waitUntilTableExists(referTopic);
        assertCount(referTopic, 1);
        assertThat(query("SELECT str, refer.str, refer.nestedObj.nestedString FROM " + toDoubleQuoted(referTopic)))
                .matches("VALUES (VARCHAR 'reference-string', VARCHAR 'test-string', VARCHAR 'nested-string')");
        assertThat(query("SELECT refer.nestedObj.arrObject[1].arrInt[1], refer.nestedObj.arrObject[1].arrInt[2], refer.nestedObj.arrObject[1].arrInt[3], refer.nestedObj.arrObject[1].nestedArrayString FROM " + toDoubleQuoted(referTopic)))
                .matches("VALUES (BIGINT '1', BIGINT '2', BIGINT '3', VARCHAR 'nested-array-string')");
    }

    @Test
    void testTopicWithKeySubject()
    {
        String topic = "topic-Key-Subject-" + randomNameSuffix();
        assertTopic(
                topic,
                format("SELECT \"%s-key\", col1, col2 FROM %s", topic, toDoubleQuoted(topic)),
                format("SELECT \"%s-key\", col1, col2, col3 FROM %s", topic, toDoubleQuoted(topic)),
                true,
                properties());
    }

    @Test
    void testObjectTypeKey()
    {
        String topic = "topic-Object-Type-Key-" + randomNameSuffix();
        assertNotExists(topic);

        List<ProducerRecord<Record, Record>> messages = ImmutableList.of(
                new ProducerRecord<>(topic, new InitialKeyRecord(0L, "key-0"), createRecordWithInitialSchema(0)),
                new ProducerRecord<>(topic, new InitialKeyRecord(1L, "key-1"), createRecordWithInitialSchema(1)));
        testingKafka.sendMessages(messages.stream(), properties());
        waitUntilTableExists(topic);
        assertCount(topic, 2);

        assertThat(query("SELECT key1, key2, col1, col2 FROM " + toDoubleQuoted(topic)))
                .matches("VALUES (CAST(0 as bigint), VARCHAR 'key-0', CAST(0 as bigint), VARCHAR 'string-0'), " +
                        "(CAST(1 as bigint), VARCHAR 'key-1', CAST(100 as bigint), VARCHAR 'string-1')");

        List<ProducerRecord<Record, Record>> evolvedMessages = ImmutableList.of(
                new ProducerRecord<>(topic, new EnvolvedKeyRecord(0L, "key-0", "key-0-3"), createRecordWithEvolvedSchema(0)),
                new ProducerRecord<>(topic, new EnvolvedKeyRecord(1L, "key-1", "key-1-3"), createRecordWithEvolvedSchema(1)));
        testingKafka.sendMessages(evolvedMessages.stream(), properties());
        assertCount(topic, 4);

        // old records should be preserved, and with the null value for the new key field
        assertThat(query("SELECT key1, key2, key3, col1, col2, col3 FROM " + toDoubleQuoted(topic) + " WHERE key3 IS NULL"))
                .matches("VALUES (CAST(0 as bigint), VARCHAR 'key-0', CAST (null AS VARCHAR), CAST(0 as bigint), VARCHAR 'string-0', CAST(null AS DOUBLE)), " +
                        "(CAST(1 as bigint), VARCHAR 'key-1', CAST(null AS VARCHAR), CAST(100 as bigint), VARCHAR 'string-1', CAST(null AS DOUBLE))");

        // new records should have additional key field
        assertThat(query("SELECT key1, key2, key3, col1, col2, col3 FROM " + toDoubleQuoted(topic) + " WHERE key3 IS NOT NULL"))
                .matches("VALUES (CAST(0 as bigint), VARCHAR 'key-0', VARCHAR 'key-0-3', CAST(0 as bigint), VARCHAR 'string-0', CAST(1.01 as double)), " +
                        "(CAST(1 as bigint), VARCHAR 'key-1', VARCHAR 'key-1-3', CAST(100 as bigint), VARCHAR 'string-1', CAST(1.11 as double))");
    }

    @Test
    void testTopicWithTombstone()
    {
        String topicName = "topic-tombstone-" + randomNameSuffix();

        assertNotExists(topicName);

        List<ProducerRecord<Long, Record>> messages = createMessages(topicName, 2, true);
        testingKafka.sendMessages(messages.stream(), properties());

        // sending tombstone message (null value) for existing key,
        // to be differentiated from simple null value message by message corrupted field
        testingKafka.sendMessages(LongStream.of(1).mapToObj(id -> new ProducerRecord<>(topicName, id, null)), properties());

        waitUntilTableExists(topicName);

        // tombstone message should have message corrupt field - true
        assertThat(query(format("SELECT \"%s-key\", col1, col2, _message_corrupt FROM %s", topicName, toDoubleQuoted(topicName))))
                .containsAll("VALUES (CAST(0 as bigint), CAST(0 as bigint), VARCHAR 'string-0', false), " +
                        "(CAST(1 as bigint), CAST(100 as bigint), VARCHAR 'string-1', false), " +
                        "(CAST(1 as bigint), null, null, true)");
    }

    @Test
    void testTopicWithRecordNameStrategy()
    {
        String topic = "topic-Record-Name-Strategy-" + randomNameSuffix();
        Map<String, String> properties = new HashMap<>(properties());
        properties.put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        assertTopic(
                topic,
                format("SELECT \"%1$s-key\", col1, col2 FROM \"%1$s&value-subject=%2$s\"", topic, "initial schema record"),
                format("SELECT \"%1$s-key\", col1, col2, col3 FROM \"%1$s&value-subject=%2$s\"", topic, "evolved schema record"),
                true,
                properties);
    }

    @Test
    void testTopicWithTopicRecordNameStrategy()
    {
        String topic = "topic-Topic-Record-Name-Strategy-" + randomNameSuffix();
        Map<String, String> properties = new HashMap<>(properties());
        properties.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

        assertTopic(
                topic,
                format("SELECT \"%1$s-key\", col1, col2 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, "initial schema record"),
                format("SELECT \"%1$s-key\", col1, col2, col3 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, "evolved schema record"),
                true,
                properties);
    }

    @Test
    void testTopicWithAllNullValues()
    {
        String topicName = "topic-all-null-values-" + randomNameSuffix();

        assertNotExists(topicName);

        List<ProducerRecord<Long, Record>> messages = createMessages(topicName, 2, true);
        testingKafka.sendMessages(messages.stream(), properties());

        // sending all null values for existing key,
        // to be differentiated from tombstone by message corrupted field
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topicName, 1, new InitialSchemaRecord(null, null))), properties());

        waitUntilTableExists(topicName);

        // simple all null values message should have message corrupt field - false
        assertThat(query(format("SELECT \"%s-key\", col1, col2, _message_corrupt FROM %s", topicName, toDoubleQuoted(topicName))))
                .containsAll("VALUES (CAST(0 as bigint), CAST(0 as bigint), VARCHAR 'string-0', false), " +
                        "(CAST(1 as bigint), CAST(100 as bigint), VARCHAR 'string-1', false), " +
                        "(CAST(1 as bigint), null, null, false)");
    }

    @Test
    void testUnsupportedInsert()
    {
        String topicName = "topic-unsupported-insert-" + randomNameSuffix();

        assertNotExists(topicName);

        List<ProducerRecord<Long, Record>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        testingKafka.sendMessages(messages.stream(), properties());

        waitUntilTableExists(topicName);

        assertThatThrownBy(() -> getQueryRunner().execute(format("INSERT INTO %s VALUES(bigint '0', varchar 'x', bigint '1')", toDoubleQuoted(topicName))))
                .hasMessage("Insert not supported");
    }

    /**
     * Test for UBS mock data, see schema1 in the MockData_JsonSchema.pdf from https://starburstdata.atlassian.net/browse/PI-1411
     */
    @Test
    void testUbsMockData()
            throws Exception
    {
        String data =
                """
                {
                     "type": "FEE_PB",
                     "source": "SBE",
                     "time": 1755498206356,
                     "data": {
                         "accrualKey": "PB_123456",
                         "accrualDate": "2025-08-12",
                         "productTypeCode": "PB",
                         "accrualType": "SCI",
                         "accountIdentifierType": "CCONSOL",
                         "accountIdentifier": "123456",
                         "dayCount": 365,
                         "dayCountMethod": "",
                         "accrualRate": -0.3,
                         "accrualAmount": 126.91,
                         "accrualCurrency": "USD",
                         "accrualStatus": "L",
                         "accrualId": "123456",
                         "postingAccountIdentifierType": "CCONSOL",
                         "postingAccountIdentifier": "123456",
                         "payDate": "2024-10-28",
                         "settlementCurrency": "USD",
                         "securityIdentifierType": "SEDOL",
                         "securityIdentifier": "123456",
                         "tradeIdentifier": 3456,
                         "settledQuantity": -80000,
                         "valuationCurrency": "USD",
                         "fxRate": 1,
                         "priceType": "COB",
                         "price": 193.01,
                         "market": "US",
                         "submarket": "US",
                         "securityStatus": "GC",
                         "accrualBenchmarkRate": 193.01,
                         "accrualSpread": 193.01
                     }
                 }
                """;
        String topicName = "topic-UbsMockData-" + randomNameSuffix();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topicName, 1L, MAPPER.readValue(data, UbsMockSecurityAccrualCommand.class))), properties());
        waitUntilTableExists(topicName);
        assertCount(topicName, 1);

        String dataColumnType = "row(\"fxRate\" double, \"dayCount\" bigint, \"settledQuantity\" double, \"postingAccountIdentifier\" varchar, \"accountIdentifier\" varchar, " +
                "\"accrualRate\" double, \"securityStatus\" varchar, \"postingAccountIdentifierType\" varchar, \"price\" double, \"accrualId\" varchar, \"accrualDate\" date, " +
                "\"tradeIdentifier\" bigint, \"dayCountMethod\" varchar, \"accrualBenchmarkRate\" double, \"accrualKey\" varchar, \"valuationCurrency\" varchar, \"priceType\" varchar, " +
                "\"accountIdentifierType\" varchar, \"submarket\" varchar, \"accrualStatus\" varchar, \"accrualType\" varchar, \"accrualCurrency\" varchar, \"market\" varchar, \"accrualAmount\" double, " +
                "\"settlementCurrency\" varchar, \"securityIdentifier\" varchar, \"accrualSpread\" double, \"securityIdentifierType\" varchar, \"productTypeCode\" varchar, \"payDate\" date)";
        assertThat(query("SHOW COLUMNS FROM " + toDoubleQuoted(topicName)))
                .skippingTypesCheck()
                .matches("VALUES ('%s-key', 'bigint', '', ''), ".formatted(topicName.toLowerCase(ENGLISH)) +
                        "('type', 'varchar', '', ''), " +
                        "('source', 'varchar', '', ''), " +
                        "('time', 'bigint', '', ''), " +
                        "('data', '" + dataColumnType + "', '', '')");
        assertThat(query("SELECT type, source, time FROM " + toDoubleQuoted(topicName)))
                .matches("VALUES (VARCHAR 'FEE_PB', VARCHAR 'SBE', BIGINT '1755498206356')");
        assertThat(query("SELECT data.fxRate, data.dayCount, data.settledQuantity, data.postingAccountIdentifier, data.accountIdentifier," +
                "data.accrualRate, data.securityStatus, data.postingAccountIdentifierType, data.price, data.accrualId, data.accrualDate," +
                "data.tradeIdentifier, data.dayCountMethod, data.accrualBenchmarkRate, data.accrualKey, data.valuationCurrency, data.priceType," +
                "data.accountIdentifierType, data.submarket, data.accrualStatus, data.accrualType, data.accrualCurrency, data.market, data.accrualAmount," +
                "data.settlementCurrency, data.securityIdentifier, data.accrualSpread, data.securityIdentifierType, data.productTypeCode, data.payDate " +
                "FROM " + toDoubleQuoted(topicName)))
                .matches("VALUES (DOUBLE '1', BIGINT '365', DOUBLE '-80000', VARCHAR '123456', VARCHAR '123456'," +
                        "DOUBLE '-0.3', VARCHAR 'GC', VARCHAR 'CCONSOL', DOUBLE '193.01', VARCHAR '123456', DATE '2025-08-12'," +
                        "BIGINT '3456', VARCHAR '', DOUBLE '193.01', VARCHAR 'PB_123456', VARCHAR 'USD', VARCHAR 'COB'," +
                        "VARCHAR 'CCONSOL', VARCHAR 'US', VARCHAR 'L', VARCHAR 'SCI', VARCHAR 'USD', VARCHAR 'US', DOUBLE '126.91'," +
                        "VARCHAR 'USD', VARCHAR '123456', DOUBLE '193.01', VARCHAR 'SEDOL', VARCHAR 'PB', DATE '2024-10-28')");

        data =
                """
                {
                    "type": "NSF_SWAP",
                    "source": "NSF",
                    "time": 1755498134405,
                    "data": {
                        "accrualKey": "SWAP_test-psds-nsf-6",
                        "accrualDate": "2025-08-17",
                        "productTypeCode": "SWAP",
                        "accrualType": "NSF",
                        "accountIdentifierType": "WRAPPER",
                        "accountIdentifier": "583313",
                        "dayCount": 365,
                        "dayCountMethod": "TEST",
                        "accrualRate": -0.3,
                        "accrualAmount": 126.91,
                        "accrualCurrency": "USD",
                        "accrualStatus": "L",
                        "accrualId": "test-psds-nsf-6",
                        "postingAccountIdentifierType": "CCONSOL",
                        "postingAccountIdentifier": "TEST",
                        "payDate": "2024-10-28",
                        "settlementCurrency": "USD",
                        "securityIdentifierType": "SEDOL",
                        "securityIdentifier": "12345",
                        "tradeIdentifier": 3456,
                        "settledQuantity": -80000,
                        "valuationCurrency": "USD",
                        "fxRate": 1,
                        "priceType": "COB",
                        "price": 193.01,
                        "submarket": "US",
                        "securityStatus": "GC",
                        "accrualBenchmarkRate": 193.01,
                        "accrualSpread": 193.01,
                        "totalLongQuantity": 193.01,
                        "totalShortQuantity": 193.01,
                        "nettedQuantity": 193.01
                    }
                }
                """;
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topicName, 2L, MAPPER.readValue(data, UbsMockSecurityAccrualCommand.class))), properties());
        assertCount(topicName, 2);

        assertThat(query("SELECT type, source, time FROM " + toDoubleQuoted(topicName)))
                .matches("VALUES (VARCHAR 'FEE_PB', VARCHAR 'SBE', BIGINT '1755498206356'), " +
                        "(VARCHAR 'NSF_SWAP', VARCHAR 'NSF', BIGINT '1755498134405')");
        assertThat(query("SELECT data.fxRate, data.dayCount, data.settledQuantity, data.postingAccountIdentifier, data.accountIdentifier," +
                "data.accrualRate, data.securityStatus, data.postingAccountIdentifierType, data.price, data.accrualId, data.accrualDate," +
                "data.tradeIdentifier, data.dayCountMethod, data.accrualBenchmarkRate, data.accrualKey, data.valuationCurrency, data.priceType," +
                "data.accountIdentifierType, data.submarket, data.accrualStatus, data.accrualType, data.accrualCurrency, data.market, data.accrualAmount," +
                "data.settlementCurrency, data.securityIdentifier, data.accrualSpread, data.securityIdentifierType, data.productTypeCode, data.payDate " +
                "FROM " + toDoubleQuoted(topicName) + " WHERE time = 1755498134405"))
                .matches("VALUES (DOUBLE '1', BIGINT '365', DOUBLE '-80000', VARCHAR 'TEST', VARCHAR '583313'," +
                        "DOUBLE '-0.3', VARCHAR 'GC', VARCHAR 'CCONSOL', DOUBLE '193.01', VARCHAR 'test-psds-nsf-6', DATE '2025-08-17'," +
                        "BIGINT '3456', VARCHAR 'TEST', DOUBLE '193.01', VARCHAR 'SWAP_test-psds-nsf-6', VARCHAR 'USD', VARCHAR 'COB'," +
                        "VARCHAR 'WRAPPER', VARCHAR 'US', VARCHAR 'L', VARCHAR 'NSF', VARCHAR 'USD', CAST(null AS VARCHAR), DOUBLE '126.91'," +
                        "VARCHAR 'USD', VARCHAR '12345', DOUBLE '193.01', VARCHAR 'SEDOL', VARCHAR 'SWAP', DATE '2024-10-28')");
    }

    private Map<String, String> properties()
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString())
                .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName())
                .buildOrThrow();
    }

    private void assertTopic(
            String topicName,
            String initialQuery,
            String evolvedQuery,
            boolean isKeyIncluded,
            Map<String, String> producerConfig)
    {
        assertNotExists(topicName);

        List<ProducerRecord<Long, Record>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        testingKafka.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, INITIAL_SCHEMA, isKeyIncluded, topicName + "-key"));

        List<ProducerRecord<Long, Record>> newMessages = createMessages(topicName, MESSAGE_COUNT, false);
        testingKafka.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<Long, Record>> allMessages = ImmutableList.<ProducerRecord<Long, Record>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertThat(query(evolvedQuery))
                .skippingTypesCheck()
                .matches(getExpectedValues(allMessages, EVOLVED_SCHEMA, isKeyIncluded, topicName + "-key"));
    }

    private void assertCount(String tableName, long count)
    {
        assertThat(computeScalar("SELECT count(*) FROM " + toDoubleQuoted(tableName))).isEqualTo(count);
    }

    private void assertNotExists(String tableName)
    {
        if (schemaExists()) {
            assertQueryReturnsEmptyResult("SHOW TABLES LIKE '" + tableName + "'");
        }
    }

    private static String getExpectedValues(List<ProducerRecord<Long, Record>> messages, ObjectSchema schema, boolean isKeyIncluded, String subject)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        Map<String, Type> types = new JsonSchemaConverter(EmptyFieldStrategy.IGNORE).convertJsonSchema(schema, subject).stream()
                .sorted(Comparator.comparing(KafkaTopicFieldDescription::name))
                .collect(toImmutableMap(KafkaTopicFieldDescription::name, KafkaTopicFieldDescription::type));
        for (ProducerRecord<Long, Record> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                columnsBuilder.add(format("CAST('%s' as bigint)", message.key()));
            }

            addExpectedColumns(types, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private static void addExpectedColumns(Map<String, Type> types, Record record, ImmutableList.Builder<String> columnsBuilder)
    {
        Map<String, Object> recordMap = record.toMap();
        for (Map.Entry<String, Type> entry : types.entrySet()) {
            String columnName = entry.getKey();
            Type type = entry.getValue();
            Object value = recordMap.get(columnName);
            if (value == null) {
                columnsBuilder.add("CAST(NULL AS " + type.getDisplayName() + ")");
            }
            else {
                columnsBuilder.add("CAST('%s' AS %s)".formatted(value, type.getDisplayName()));
            }
        }
    }

    private void waitUntilTableExists(String tableName)
    {
        Failsafe.with(
                        RetryPolicy.builder()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100))
                                .build())
                .run(() -> assertThat(schemaExists()).isTrue());
        Failsafe.with(
                        RetryPolicy.builder()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100))
                                .build())
                .run(() -> assertThat(getQueryRunner().tableExists(getSession(), tableName.toLowerCase(ENGLISH))).isTrue());
    }

    private boolean schemaExists()
    {
        return computeActual(format(
                "SHOW SCHEMAS FROM %s LIKE '%s'",
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow()))
                .getRowCount() == 1;
    }

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private static List<ProducerRecord<Long, Record>> createMessages(String topicName, int messageCount, boolean useInitialSchema)
    {
        ImmutableList.Builder<ProducerRecord<Long, Record>> producerRecordBuilder = ImmutableList.builder();
        if (useInitialSchema) {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, key, createRecordWithInitialSchema(key)));
            }
        }
        else {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, key, createRecordWithEvolvedSchema(key)));
            }
        }
        return producerRecordBuilder.build();
    }

    private interface Record
    {
        Map<String, Object> toMap();
    }

    private record InitialKeyRecord(Long key1, String key2)
            implements Record
    {
        @Override
        public Map<String, Object> toMap()
        {
            return ImmutableMap.of("key1", key1, "key2", key2);
        }
    }

    private record EnvolvedKeyRecord(Long key1, String key2, String key3)
            implements Record
    {
        @Override
        public Map<String, Object> toMap()
        {
            return ImmutableMap.of("key1", key1, "key2", key2, "key3", key3);
        }
    }

    private record InitialSchemaRecord(Long col1, String col2)
            implements Record
    {
        @Override
        public Map<String, Object> toMap()
        {
            return ImmutableMap.of("col1", col1, "col2", col2);
        }
    }

    private record EvolvedSchemaRecord(Long col1, String col2, Double col3)
            implements Record
    {
        @Override
        public Map<String, Object> toMap()
        {
            return ImmutableMap.of("col1", col1, "col2", col2, "col3", col3);
        }
    }

    private static Record createRecordWithInitialSchema(long key)
    {
        return new InitialSchemaRecord(multiplyExact(key, 100), format("string-%s", key));
    }

    private static Record createRecordWithEvolvedSchema(long key)
    {
        return new EvolvedSchemaRecord(multiplyExact(key, 100), format("string-%s", key), (key + 10.1d) / 10.0d);
    }

    @Schema(value = """
                    {
                         "$schema": "http://json-schema.org/draft-07/schema#",
                             "type": "object",
                             "properties": {
                                 "country": {
                                     "type": "string",
                                     "const": "USA"
                                 }
                             },
                         "required": ["country"],
                         "additionalProperties": false
                     }
                    """, refs = {})
    private record ConstEvent(String country) {}

    // use annotation to define the schema with timestamp field, the prue json serializer not
    // support passing format info
    @Schema(value = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "type": "object",
                      "properties": {
                        "timestamp": {
                          "type": "string",
                          "format": "date-time"
                        }
                      }
                    }""", refs = {})
    private record TimestampEvent(String timestamp) {}

    @Schema(value = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "type": "object",
                      "properties": {
                        "date": {
                          "type": "string",
                          "format": "date"
                        },
                        "time": {
                          "type": "string",
                          "format": "time"
                        }
                      }
                    }""", refs = {})
    private record DateTimeEvent(String date, String time) {}

    // use annotation to define the schema with timestamp field, the prue json serializer not
    // support passing format info
    @Schema(value = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "type": "object",
                      "properties": {
                        "bool": {
                          "type": "boolean"
                        },
                        "arrInt": {
                          "type": "array",
                          "items": {
                            "type": "integer",
                            "minimum": -2147483648,
                            "maximum": 2147483647
                          }
                        },
                        "arrString": {
                          "type": "array",
                          "items": {
                            "type": "string"
                          }
                        },
                        "arrTimestamp": {
                          "type": "array",
                          "items": {
                            "type": "string",
                            "format": "date-time"
                          }
                        },
                        "arrDate": {
                          "type": "array",
                          "items": {
                            "type": "string",
                            "format": "date"
                          }
                        },
                        "arrTime": {
                          "type": "array",
                          "items": {
                            "type": "string",
                            "format": "time"
                          }
                        }
                      }
                    }""", refs = {})
    private record ArrayEvent(boolean bool, List<Integer> arrInt, List<String> arrString, List<String> arrTimestamp, List<String> arrDate, List<String> arrTime) {}

    // use annotation to define the schema with array column, the prue json serializer not
    // support passing minimum, maximum info
    @Schema(value = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "title": "object_event",
                      "type": "object",
                      "properties": {
                        "str": {
                          "type": "string"
                        },
                        "nestedObj": {
                            "type": "object",
                            "properties": {
                                "arrObject" : {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "arrInt": {
                                                "type": "array",
                                                "items": {
                                                    "type": "integer",
                                                    "minimum": -2147483648,
                                                    "maximum": 2147483647
                                                }
                                            },
                                            "nestedArrayString": {
                                                "type": "string"
                                            }
                                        }
                                    }
                                },
                                "nestedString": {
                                    "type": "string"
                                }
                            }
                        }
                      }
                    }""", refs = {})
    private record ObjectEvent(String str, NestedObjectEvent nestedObj) {}

    private record NestedObjectEvent(List<NestedObject> arrObject, String nestedString) {}

    private record NestedObject(List<Integer> arrInt, String nestedArrayString) {}

    // use annotation to define the schema with reference column, the prue json serializer not
    // support passing reference info
    @Schema(value = """
                    {
                      "$schema": "http://json-schema.org/draft-07/schema#",
                      "title": "reference_event",
                      "type": "object",
                      "properties": {
                        "str": {
                          "type": "string"
                        },
                        "refer": {
                            "$ref": "#/refs/object_event"
                        }
                      },
                      "refs": {
                        "object_event": {
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "title": "object_event",
                            "type": "object",
                            "properties": {
                                "str": {
                                    "type": "string"
                                },
                                "nestedObj": {
                                    "type": "object",
                                    "properties": {
                                        "arrObject" : {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "arrInt": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "integer",
                                                            "minimum": -2147483648,
                                                            "maximum": 2147483647
                                                        }
                                                    },
                                                    "nestedArrayString": {
                                                        "type": "string"
                                                    }
                                                }
                                            }
                                        },
                                        "nestedString": {
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        }
                      }
                    }""", refs = {})
    private record ReferenceEvent(String str, ObjectEvent refer) {}
}
