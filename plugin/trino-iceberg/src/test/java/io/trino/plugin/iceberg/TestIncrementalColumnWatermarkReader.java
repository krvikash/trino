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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.StorageTableWatermark;
import io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.StoredWatermark;
import io.trino.spi.connector.IncrementalColumnWatermark;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeId;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkBuilder.encodeWatermark;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.TRINO_INCREMENTAL_COLUMN_WATERMARK;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.decodeWatermark;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.parseStoredWatermark;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.readIncrementalColumnWatermark;
import static io.trino.plugin.iceberg.TestIncrementalColumn.find;
import static io.trino.spi.connector.IncrementalColumnWatermark.Empty.emptyIncrementalColumnWatermark;
import static io.trino.spi.connector.IncrementalColumnWatermark.Unknown.unknownIncrementalColumnWatermark;
import static io.trino.spi.connector.IncrementalColumnWatermark.Value.resolvedIncrementalColumnWatermark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrementalColumnWatermarkReader
{
    private static final JsonCodec<StoredWatermark> STORED_WATERMARK_CODEC = jsonCodec(StoredWatermark.class);
    private static final IncrementalColumnWatermarkReader READER = new IncrementalColumnWatermarkReader(TESTING_TYPE_MANAGER);

    @Test
    public void testWatermarkRoundTrips()
    {
        assertRoundTrips(find("int_col"), 42L);
        assertRoundTrips(find("int_col"), (long) Integer.MIN_VALUE);
        assertRoundTrips(find("bigint_col"), 200L);
        assertRoundTrips(find("bigint_col"), -200L);
        assertRoundTrips(find("bigint_col"), Long.MAX_VALUE);
        assertRoundTrips(find("bigint_col"), Long.MIN_VALUE);
        assertRoundTrips(find("date_col"), 20_000L);
        assertRoundTrips(find("date_col"), -1L);
        assertRoundTrips(find("short_decimal_col"), 1_234_567L);
        assertRoundTrips(find("short_decimal_col"), -1_234_567L);
        assertRoundTrips(find("long_decimal_col"), Int128.valueOf(new BigInteger("1234567890123456789012345678")));
        assertRoundTrips(find("long_decimal_col"), Int128.valueOf(new BigInteger("-1234567890123456789012345678")));
        assertRoundTrips(find("time_col"), 86_399_999_999_000_000L);
        assertRoundTrips(find("timestamp_col"), 1_767_225_600_123_456L);
        assertRoundTrips(find("timestamp_col"), -1L);
    }

    @Test
    public void testWatermarkIsStoredWithFieldIdAndStorageType()
    {
        assertThat(parseStoredWatermark(encodeWatermark(find("bigint_col"), 200L)))
                .isEqualTo(new StoredWatermark(2, TypeId.of("bigint"), "200"));
        assertThat(parseStoredWatermark(encodeWatermark(find("long_decimal_col"), Int128.valueOf(new BigInteger("-1234567890123456789012345678")))))
                .isEqualTo(new StoredWatermark(5, TypeId.of("decimal(38,10)"), "-1234567890123456789012345678"));
        assertThat(parseStoredWatermark(encodeWatermark(find("timestamp_col"), 1_767_225_600_123_000L)))
                .isEqualTo(new StoredWatermark(7, TypeId.of("timestamp(6)"), "1767225600123000"));
    }

    /**
     * A {@code TIMESTAMP(3)} view column is materialized into an Iceberg {@code timestamp}, which is
     * microsecond precision, so the watermark is of the storage type and carries epoch microseconds.
     * Rendering it at the view's precision is the engine's job.
     */
    @Test
    public void testTimestampMillisViewColumnWatermarkIsOfStorageType()
    {
        IncrementalColumn timestampColumn = find("timestamp_col");
        assertThat(timestampColumn.trinoType()).isEqualTo(TIMESTAMP_MICROS);

        long epochMicros = 1_767_225_600_123_000L;
        assertThat(readIncrementalColumnWatermark(summaryWithWatermark(encodeWatermark(timestampColumn, epochMicros)), timestampColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(resolvedIncrementalColumnWatermark(TIMESTAMP_MICROS, epochMicros));

        String watermarkOfViewType = storedWatermark(7, createTimestampType(3).getTypeId(), "1767225600123000");
        assertThat(decodeWatermark(watermarkOfViewType, timestampColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(readIncrementalColumnWatermark(summaryWithWatermark(watermarkOfViewType), timestampColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(unknownIncrementalColumnWatermark());
    }

    @Test
    public void testWatermarkForAnotherColumnOrTypeIsDiscarded()
    {
        IncrementalColumn bigintColumn = find("bigint_col");
        String watermark = encodeWatermark(bigintColumn, 200L);

        assertThat(decodeWatermark(watermark, new IncrementalColumn("bigint_col", find("int_col").field(), BIGINT), TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(watermark, find("int_col"), TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(watermark, find("date_col"), TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(storedWatermark(2, createDecimalType(18, 0).getTypeId(), "200"), bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(storedWatermark(6, TIME_MICROS.getTypeId(), "200"), find("timestamp_col"), TESTING_TYPE_MANAGER)).isEmpty();
    }

    @Test
    public void testMalformedWatermarkIsDiscarded()
    {
        IncrementalColumn bigintColumn = find("bigint_col");
        assertThat(decodeWatermark("not json", bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark("{}", bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark("{\"fieldId\":2,\"type\":\"bigint\"}", bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(storedWatermark(2, BIGINT.getTypeId(), "not a number"), bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(storedWatermark(2, BIGINT.getTypeId(), "1.5"), bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
        assertThat(decodeWatermark(storedWatermark(2, TypeId.of("no_such_type"), "200"), bigintColumn, TESTING_TYPE_MANAGER)).isEmpty();
    }

    @Test
    public void testReadWatermarkFromSnapshotSummary()
    {
        IncrementalColumn bigintColumn = find("bigint_col");

        assertThat(readIncrementalColumnWatermark(summaryWithWatermark(encodeWatermark(bigintColumn, 200L)), bigintColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(resolvedIncrementalColumnWatermark(BIGINT, 200L));
        assertThat(readIncrementalColumnWatermark(ImmutableMap.of(TOTAL_RECORDS_PROP, "0"), bigintColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(emptyIncrementalColumnWatermark());
        assertThat(readIncrementalColumnWatermark(ImmutableMap.of(TOTAL_RECORDS_PROP, "3"), bigintColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(unknownIncrementalColumnWatermark());
        assertThat(readIncrementalColumnWatermark(ImmutableMap.of(), bigintColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(unknownIncrementalColumnWatermark());
        assertThat(readIncrementalColumnWatermark(summaryWithWatermark("not json"), bigintColumn, TESTING_TYPE_MANAGER))
                .isEqualTo(unknownIncrementalColumnWatermark());
    }

    @Test
    public void testReadWatermarkFromStorageTableSnapshot()
    {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", ImmutableMap.of());
            catalog.createNamespace(Namespace.of("schema"));
            Table storageTable = catalog.createTable(TableIdentifier.of("schema", "storage"), new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "event_timestamp", Types.LongType.get()),
                    Types.NestedField.optional(3, "name", Types.StringType.get())));
            IncrementalColumn eventTimestampColumn = new IncrementalColumn("event_timestamp", storageTable.schema().findField("event_timestamp"), BIGINT);

            assertThat(READER.readWatermark("event_timestamp", storageTable, OptionalLong.empty()))
                    .isEqualTo(new StorageTableWatermark(Optional.of(eventTimestampColumn), emptyIncrementalColumnWatermark()));

            appendFile(storageTable, Optional.of(encodeWatermark(eventTimestampColumn, 20L)));
            long withWatermark = storageTable.currentSnapshot().snapshotId();
            appendFile(storageTable, Optional.empty());
            long withoutWatermark = storageTable.currentSnapshot().snapshotId();

            assertThat(READER.readWatermark("event_timestamp", storageTable, OptionalLong.of(withWatermark)))
                    .isEqualTo(new StorageTableWatermark(Optional.of(eventTimestampColumn), resolvedIncrementalColumnWatermark(BIGINT, 20L)));
            assertThat(READER.readWatermark("event_timestamp", storageTable, OptionalLong.of(withoutWatermark)))
                    .isEqualTo(new StorageTableWatermark(Optional.of(eventTimestampColumn), unknownIncrementalColumnWatermark()));
            assertThat(READER.readWatermark("event_timestamp", storageTable, OptionalLong.of(withWatermark + withoutWatermark)))
                    .isEqualTo(new StorageTableWatermark(Optional.of(eventTimestampColumn), unknownIncrementalColumnWatermark()));

            assertThat(READER.readWatermark("name", storageTable, OptionalLong.of(withWatermark)))
                    .isEqualTo(new StorageTableWatermark(Optional.empty(), unknownIncrementalColumnWatermark()));
            assertThat(READER.readWatermark("missing", storageTable, OptionalLong.empty()))
                    .isEqualTo(new StorageTableWatermark(Optional.empty(), emptyIncrementalColumnWatermark()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void appendFile(Table table, Optional<String> watermark)
    {
        AppendFiles append = table.newFastAppend()
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/" + randomNameSuffix() + ".parquet")
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(10)
                        .withRecordCount(1)
                        .build());
        watermark.ifPresent(value -> append.set(TRINO_INCREMENTAL_COLUMN_WATERMARK, value));
        append.commit();
    }

    private static void assertRoundTrips(IncrementalColumn incrementalColumn, Object value)
    {
        String watermark = encodeWatermark(incrementalColumn, value);
        assertThat(decodeWatermark(watermark, incrementalColumn, TESTING_TYPE_MANAGER)).contains(value);
        IncrementalColumnWatermark read = readIncrementalColumnWatermark(summaryWithWatermark(watermark), incrementalColumn, TESTING_TYPE_MANAGER);
        assertThat(read).isEqualTo(resolvedIncrementalColumnWatermark(incrementalColumn.trinoType(), value));
    }

    private static Map<String, String> summaryWithWatermark(String watermark)
    {
        return ImmutableMap.of(TRINO_INCREMENTAL_COLUMN_WATERMARK, watermark, TOTAL_RECORDS_PROP, "3");
    }

    private static String storedWatermark(int fieldId, TypeId type, String value)
    {
        return STORED_WATERMARK_CODEC.toJson(new StoredWatermark(fieldId, type, value));
    }
}
