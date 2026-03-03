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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.decoder.json.JsonRowDecoder;
import io.trino.decoder.json.JsonRowDecoderFactory;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.spi.type.Type;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.internal.DateFormatValidator;
import org.everit.json.schema.internal.DateTimeFormatValidator;
import org.everit.json.schema.internal.TimeFormatValidator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.decoder.util.DecoderTestUtil.TESTING_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;
import static java.time.temporal.ChronoField.INSTANT_SECONDS;
import static java.time.temporal.ChronoField.MILLI_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJsonConfluentRowDecoder
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testDecodingRows()
            throws Exception
    {
        ObjectSchema initialSchema = ObjectSchema.builder()
                .addPropertySchema("col1", NumberSchema.builder().requiresInteger(true).build())
                .addPropertySchema("col2", StringSchema.builder().nullable(true).build())
                .addPropertySchema("col3", NumberSchema.builder().requiresInteger(true).defaultValue(42).build())
                .addPropertySchema("col4", NumberSchema.builder().requiresInteger(true).nullable(true).build())
                .addPropertySchema("col5", StringSchema.builder().nullable(true).build())
                .addPropertySchema("col6", StringSchema.builder().formatValidator(new DateFormatValidator()).nullable(true).build())
                .addPropertySchema("col7", StringSchema.builder().formatValidator(new TimeFormatValidator()).nullable(true).build())
                .addPropertySchema("col8", StringSchema.builder().formatValidator(new DateTimeFormatValidator()).nullable(true).build())
                .build();

        ObjectSchema evolvedSchema = ObjectSchema.builder()
                .addPropertySchema("col1", NumberSchema.builder().requiresInteger(true).build())
                .addPropertySchema("col2", StringSchema.builder().nullable(true).build())
                .addPropertySchema("col3", NumberSchema.builder().requiresInteger(true).defaultValue(3).build())
                .addPropertySchema("col4", NumberSchema.builder().requiresInteger(true).nullable(true).build())
                .addPropertySchema("col5", StringSchema.builder().nullable(true).build())
                .addPropertySchema("col6", NumberSchema.builder().requiresInteger(false).nullable(true).build())
                .build();

        Set<DecoderColumnHandle> columnHandles = ImmutableSet.<DecoderColumnHandle>builder()
                .add(new KafkaColumnHandle("col1", BIGINT, "col1", null, null, false, false, false))
                .add(new KafkaColumnHandle("col2", VARCHAR, "col2", null, null, false, false, false))
                .add(new KafkaColumnHandle("col3", BIGINT, "col3", null, null, false, false, false))
                .add(new KafkaColumnHandle("col4", BIGINT, "col4", null, null, false, false, false))
                .add(new KafkaColumnHandle("col5", VARCHAR, "col5", null, null, false, false, false))
                .add(new KafkaColumnHandle("col6", BIGINT, "col6", null, null, false, false, false))
                .build();

        RowDecoder rowDecoder = getRowDecoder(columnHandles);
        testRow(rowDecoder, generateJsonMessage(initialSchema, Arrays.asList(3, "string-3", 30, 300, new String(new byte[] {1, 2, 3}, UTF_8))));
        testRow(rowDecoder, generateJsonMessage(initialSchema, Arrays.asList(3, "", 30, null, null)));
        testRow(rowDecoder, generateJsonMessage(initialSchema, Arrays.asList(3, "\u0394\u66f4\u6539", 30, null, new String(new byte[] {1, 2, 3}, UTF_8))));
        testRow(rowDecoder, generateJsonMessage(evolvedSchema, Arrays.asList(4, "string-4", 40, 400, null, 4L)));
        testRow(rowDecoder, generateJsonMessage(evolvedSchema, Arrays.asList(5, "string-5", 50, 500, new String(new byte[] {1, 2, 3}, UTF_8), null)));
    }

    @Test
    void testSingleValueRow()
            throws Exception
    {
        assertRow(BOOLEAN, true);
        assertRow(INTEGER, 3);
        assertRow(BIGINT, 3L);
        assertRow(VARCHAR, "string-3");
        assertRow(DOUBLE, 3.0);
        assertRow(DATE, "2024-01-03");
        assertRow(TIME_TZ_MILLIS, "12:34:56.789Z");
        assertRow(TIMESTAMP_TZ_MILLIS, "2024-01-03T12:34:56.789Z");
    }

    private static void assertRow(Type type, Object value)
            throws Exception
    {
        String dateFormat = (type.equals(DATE) || type.equals(TIME_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MILLIS)) ? "iso8601" : null;
        Set<DecoderColumnHandle> columnHandles = ImmutableSet.of(new KafkaColumnHandle("col", type, "col", dateFormat, null, false, false, false));
        RowDecoder rowDecoder = getRowDecoder(columnHandles);
        testSingleValueRow(rowDecoder, MAPPER.writeValueAsString(ImmutableMap.of("col", value)), value);
    }

    private static void testSingleValueRow(RowDecoder rowDecoder, String record, Object value)
    {
        byte[] serializedRecord = serialize(record);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(serializedRecord);
        checkState(decodedRow.isPresent(), "decodedRow is not present");
        Map.Entry<DecoderColumnHandle, FieldValueProvider> entry = getOnlyElement(decodedRow.get().entrySet());
        assertValuesAreEqual(entry.getKey().getType(), entry.getValue(), value);
    }

    private static void testRow(RowDecoder rowDecoder, String record)
            throws JsonProcessingException
    {
        byte[] serializedRecord = serialize(record);
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(serializedRecord);
        assertRowsAreEqual(decodedRow, record);
    }

    private static byte[] serialize(String record)
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // write magic byte
            out.write((byte) 0);

            // write schema id
            out.write(Ints.toByteArray(1));

            // write data
            out.write(record.getBytes(UTF_8));

            return out.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static RowDecoder getRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        return getJsonRowDecoderFactory().create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, ImmutableMap.of(), columnHandles));
    }

    public static JsonRowDecoderFactory getJsonRowDecoderFactory()
    {
        return new ConfluentJsonRowDecoderFactory(new ConfluentSchemaRegistryJsonPayloadProvider(new JsonMapper()));
    }

    private static void assertRowsAreEqual(Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow, String expected)
            throws JsonProcessingException
    {
        checkState(decodedRow.isPresent(), "decoded row is not present");
        Map<String, Object> expectedValues = MAPPER.readValue(expected, Map.class);
        for (Map.Entry<DecoderColumnHandle, FieldValueProvider> entry : decodedRow.get().entrySet()) {
            String columnName = entry.getKey().getName();
            assertValuesAreEqual(entry.getKey().getType(), entry.getValue(), expectedValues.get(columnName));
        }
    }

    private static void assertValuesAreEqual(Type type, FieldValueProvider actual, Object expected)
    {
        if (actual.isNull()) {
            assertThat(expected).isNull();
            return;
        }

        if (type.equals(BOOLEAN)) {
            assertThat(actual.getBoolean()).isEqualTo(expected);
        }
        else if (type.equals(BIGINT)) {
            assertThat(actual.getLong()).isEqualTo(((Number) expected).longValue());
        }
        else if (type.equals(INTEGER)) {
            assertThat(actual.getLong()).isEqualTo(((Number) expected).intValue());
        }
        else if (type.equals(DOUBLE)) {
            assertThat(actual.getDouble()).isEqualTo(((Number) expected).doubleValue());
        }
        else if (type.equals(REAL)) {
            assertThat(actual.getLong()).isEqualTo(Float.floatToIntBits(((Number) expected).floatValue()));
        }
        else if (type.equals(VARCHAR)) {
            assertThat(actual.getSlice().toStringUtf8()).isEqualTo(expected);
        }
        else if (type.equals(DATE)) {
            assertThat(actual.getLong()).isEqualTo(LocalDate.parse((String) expected).toEpochDay());
        }
        else if (type.equals(TIME_TZ_MILLIS)) {
            TemporalAccessor parseResult = ISO_OFFSET_TIME.parse((String) expected);
            long time = packTimeWithTimeZone((long) parseResult.get(MILLI_OF_DAY) * NANOSECONDS_PER_MILLISECOND, ZoneOffset.from(parseResult).getTotalSeconds() / 60);
            assertThat(actual.getLong()).isEqualTo(time);
        }
        else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            TemporalAccessor parseResult = ISO_OFFSET_DATE_TIME.parse((String) expected);
            long time = packDateTimeWithZone(parseResult.getLong(INSTANT_SECONDS) * 1000 + parseResult.getLong(MILLI_OF_SECOND), getTimeZoneKey(ZoneId.from(parseResult).getId()));
            assertThat(actual.getLong()).isEqualTo(time);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static String generateJsonMessage(ObjectSchema schema, List<Object> values)
            throws JsonProcessingException
    {
        Map<String, Schema> propertySchemas = schema.getPropertySchemas();
        List<String> keys = propertySchemas.keySet().stream().sorted().toList();
        Map<String, Object> recordMap = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i++) {
            recordMap.put(keys.get(i), values.get(i));
        }
        return MAPPER.writeValueAsString(recordMap);
    }
}
