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
import io.trino.spi.type.Int128;
import org.apache.iceberg.Metrics;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergFileFormat.AVRO;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkBuilder.greaterOf;
import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.decodeWatermark;
import static io.trino.plugin.iceberg.TestIncrementalColumn.find;
import static io.trino.plugin.iceberg.TestIncrementalColumn.metricsWithUpperBound;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIncrementalColumnWatermarkBuilder
{
    private static final IncrementalColumn BIGINT_COLUMN = find("bigint_col");
    private static final IncrementalColumn TIMESTAMP_COLUMN = find("timestamp_col");

    @Test
    public void testGreaterOf()
    {
        assertThat(greaterOf(200L, 300L)).isEqualTo(300L);
        assertThat(greaterOf(300L, 200L)).isEqualTo(300L);
        assertThat(greaterOf(300L, 300L)).isEqualTo(300L);
        assertThat(greaterOf(-300L, -200L)).isEqualTo(-200L);
        assertThat(greaterOf(-200L, -300L)).isEqualTo(-200L);
        assertThat(greaterOf(-1L, 1L)).isEqualTo(1L);

        Int128 small = Int128.valueOf(new BigInteger("1234567890123456789012345678"));
        Int128 large = Int128.valueOf(new BigInteger("2234567890123456789012345678"));
        Int128 negative = Int128.valueOf(new BigInteger("-2234567890123456789012345678"));
        assertThat(greaterOf(small, large)).isEqualTo(large);
        assertThat(greaterOf(large, small)).isEqualTo(large);
        assertThat(greaterOf(large, large)).isEqualTo(large);
        assertThat(greaterOf(negative, small)).isEqualTo(small);
        assertThat(greaterOf(small, negative)).isEqualTo(small);
        assertThat(greaterOf(negative, Int128.valueOf(new BigInteger("-3234567890123456789012345678")))).isEqualTo(negative);
    }

    @Test
    public void testWatermarkIsMaxOfAppendedFiles()
    {
        IncrementalColumnWatermarkBuilder watermark = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.empty());
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 200L));
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 300L));
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, -100L));
        assertStoredWatermark(watermark, BIGINT_COLUMN, 300L);
    }

    @Test
    public void testWatermarkBuildsOnPreviousMaxValue()
    {
        IncrementalColumnWatermarkBuilder belowPrevious = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        belowPrevious.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 300L));
        assertStoredWatermark(belowPrevious, BIGINT_COLUMN, 500L);

        IncrementalColumnWatermarkBuilder abovePrevious = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        abovePrevious.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 700L));
        assertStoredWatermark(abovePrevious, BIGINT_COLUMN, 700L);

        IncrementalColumnWatermarkBuilder nothingAppended = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        assertStoredWatermark(nothingAppended, BIGINT_COLUMN, 500L);
    }

    @Test
    public void testLongDecimalWatermarkKeepsSignedMaxBeyondLongRange()
    {
        IncrementalColumn longDecimalColumn = find("long_decimal_col");

        IncrementalColumnWatermarkBuilder abovePrevious = new IncrementalColumnWatermarkBuilder(longDecimalColumn, Optional.of(Int128.valueOf(new BigInteger("-1"))));
        abovePrevious.acceptFile(PARQUET, metricsWithUpperBound(longDecimalColumn, new BigDecimal("1.0000000000")));
        abovePrevious.acceptFile(PARQUET, metricsWithUpperBound(longDecimalColumn, new BigDecimal("123456789012345678.0123456789")));
        abovePrevious.acceptFile(PARQUET, metricsWithUpperBound(longDecimalColumn, new BigDecimal("-123456789012345678.0123456789")));
        assertStoredWatermark(abovePrevious, longDecimalColumn, Int128.valueOf(new BigInteger("1234567890123456780123456789")));

        IncrementalColumnWatermarkBuilder belowPrevious = new IncrementalColumnWatermarkBuilder(longDecimalColumn, Optional.of(Int128.valueOf(new BigInteger("2234567890123456780123456789"))));
        belowPrevious.acceptFile(PARQUET, metricsWithUpperBound(longDecimalColumn, new BigDecimal("123456789012345678.0123456789")));
        assertStoredWatermark(belowPrevious, longDecimalColumn, Int128.valueOf(new BigInteger("2234567890123456780123456789")));
    }

    @Test
    public void testNoWatermarkWhenNothingIsMaterialized()
    {
        IncrementalColumnWatermarkBuilder watermark = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.empty());
        assertThat(watermark.build()).isEmpty();

        watermark.acceptFile(PARQUET, allNullMetrics(BIGINT_COLUMN));
        assertThat(watermark.build()).isEmpty();
    }

    @Test
    public void testAllNullFileKeepsWatermarkKnown()
    {
        IncrementalColumnWatermarkBuilder watermark = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        watermark.acceptFile(PARQUET, allNullMetrics(BIGINT_COLUMN));
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 600L));
        assertStoredWatermark(watermark, BIGINT_COLUMN, 600L);
    }

    @Test
    public void testFileWithoutUpperBoundDropsWatermark()
    {
        IncrementalColumnWatermarkBuilder watermark = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 600L));
        watermark.acceptFile(PARQUET, new Metrics(10L, null, null, ImmutableMap.of(BIGINT_COLUMN.field().fieldId(), 3L), null));
        watermark.acceptFile(PARQUET, metricsWithUpperBound(BIGINT_COLUMN, 700L));
        assertThat(watermark.build()).isEmpty();

        IncrementalColumnWatermarkBuilder withoutMetrics = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.of(500L));
        withoutMetrics.acceptFile(PARQUET, new Metrics(10L, null, null, null, null));
        assertThat(withoutMetrics.build()).isEmpty();
    }

    @Test
    public void testOrcTimestampFileDropsWatermark()
    {
        long epochMicros = 1_767_225_600_123_456L;

        IncrementalColumnWatermarkBuilder parquet = new IncrementalColumnWatermarkBuilder(TIMESTAMP_COLUMN, Optional.empty());
        parquet.acceptFile(PARQUET, metricsWithUpperBound(TIMESTAMP_COLUMN, epochMicros));
        parquet.acceptFile(AVRO, metricsWithUpperBound(TIMESTAMP_COLUMN, epochMicros - 1));
        assertStoredWatermark(parquet, TIMESTAMP_COLUMN, epochMicros);

        IncrementalColumnWatermarkBuilder orc = new IncrementalColumnWatermarkBuilder(TIMESTAMP_COLUMN, Optional.empty());
        orc.acceptFile(PARQUET, metricsWithUpperBound(TIMESTAMP_COLUMN, epochMicros));
        orc.acceptFile(ORC, metricsWithUpperBound(TIMESTAMP_COLUMN, epochMicros - 1));
        assertThat(orc.build()).isEmpty();

        IncrementalColumnWatermarkBuilder orcBigint = new IncrementalColumnWatermarkBuilder(BIGINT_COLUMN, Optional.empty());
        orcBigint.acceptFile(ORC, metricsWithUpperBound(BIGINT_COLUMN, 200L));
        assertStoredWatermark(orcBigint, BIGINT_COLUMN, 200L);
    }

    @Test
    public void testTimeWatermarkIsStoredInPicoseconds()
    {
        IncrementalColumn timeColumn = find("time_col");
        IncrementalColumnWatermarkBuilder watermark = new IncrementalColumnWatermarkBuilder(timeColumn, Optional.empty());
        watermark.acceptFile(ORC, metricsWithUpperBound(timeColumn, 3_723_000_001L));
        assertStoredWatermark(watermark, timeColumn, 3_723_000_001_000_000L);
    }

    private static void assertStoredWatermark(IncrementalColumnWatermarkBuilder watermark, IncrementalColumn incrementalColumn, Object expectedMaxValue)
    {
        Optional<String> storedWatermark = watermark.build();
        assertThat(storedWatermark).isPresent();
        assertThat(decodeWatermark(storedWatermark.orElseThrow(), incrementalColumn, TESTING_TYPE_MANAGER)).contains(expectedMaxValue);
    }

    private static Metrics allNullMetrics(IncrementalColumn incrementalColumn)
    {
        return new Metrics(10L, null, null, ImmutableMap.of(incrementalColumn.field().fieldId(), 10L), null);
    }
}
