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
package io.trino.operator.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.scalar.timestamp.DateFormat;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuDateFormat
{
    private static final long SOME_INSTANT = epochMicros(LocalDateTime.of(2026, 9, 30, 13, 2, 3)) + 456_000;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateFormat(NullsProvider nullsProvider)
    {
        // Without a year field every value is formattable, so the whole range is covered
        for (String format : List.of("%m-%d %H:%i:%s.%f", "at %h o'clock, 100%% [%S] %T %I%")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                Type timestampType = createTimestampType(precision);
                try {
                    List<Page> inputPages = List.of(new Page(64, createBlock(timestampType, 64, nullsProvider)));
                    assertGpuMatchesCpu(inputPages, List.of(timestampType), dateFormatExpression(timestampType, format));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("format: " + format));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDateFormatTimestampWithTimeZone(NullsProvider nullsProvider)
    {
        // The random instants span years beyond 9999, so the formats have no year field
        for (String format : List.of("%m-%d %H:%i:%s.%f", "%T %h")) {
            for (Type type : TESTED_GPU_TIMESTAMP_WITH_TIME_ZONE_TYPES) {
                try {
                    List<Page> inputPages = List.of(new Page(64, createBlock(type, 64, nullsProvider)));
                    assertGpuMatchesCpu(inputPages, List.of(type), dateFormatExpression(type, format));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("format: " + format));
                    t.addSuppressed(new Exception("type: " + type));
                    throw t;
                }
            }
        }
    }

    @Test
    void testDateFormatYear()
    {
        Random random = new Random(7);
        for (String format : List.of("%Y-%m-%d %H:%i:%s.%f", "%y/%m/%d", "%j", "%x-%v", "%%%Y%%")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                TimestampType timestampType = createTimestampType(precision);
                // Precisions above 6 are nanoseconds on the GPU, which span only years 1677 to 2262
                int firstYear = timestampType.isShort() ? 1 : 1678;
                int lastYear = timestampType.isShort() ? 9999 : 2261;
                long minEpochMicros = epochMicros(LocalDateTime.of(firstYear, 1, 1, 0, 0));
                long maxEpochMicros = epochMicros(LocalDateTime.of(lastYear + 1, 1, 1, 0, 0)) - 1_000;
                List<Long> epochMicros = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    long micros = random.nextLong(minEpochMicros, maxEpochMicros);
                    epochMicros.add(i % 5 == 0 ? null : Timestamps.round(micros, max(0, 6 - precision)));
                }
                try {
                    assertDateFormatMatchesCpu(timestampType, format, epochMicros);
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("format: " + format));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @Test
    void testDateFormatYearOutOfRange()
    {
        String format = "%Y-%m-%d %H:%i:%s.%f";
        long firstEpochMicros = epochMicros(LocalDateTime.of(0, 1, 1, 0, 0));
        long lastEpochMicros = epochMicros(LocalDateTime.of(10000, 1, 1, 0, 0)) - 1_000;
        for (TimestampType timestampType : List.of(createTimestampType(3), createTimestampType(6))) {
            assertDateFormatMatchesCpu(timestampType, format, List.of(firstEpochMicros, lastEpochMicros));
            assertDateFormatGpuThrows(timestampType, format, firstEpochMicros - 1_000);
            assertDateFormatGpuThrows(timestampType, format, lastEpochMicros + 1_000);
            assertDateFormatGpuThrows(timestampType, "%y", firstEpochMicros - 1_000);
            assertDateFormatMatchesCpu(timestampType, "%j", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        }
        // Rounding to milliseconds carries into year 10000
        assertDateFormatGpuThrows(createTimestampType(6), format, lastEpochMicros + 500);
        // Fields that do not depend on the year are formattable in any year
        assertDateFormatMatchesCpu(createTimestampType(6), "%m-%d %T", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        // The year of the ISO week is -1 in the first days of year 0
        long firstWeekYearEpochMicros = epochMicros(LocalDateTime.of(1, 1, 1, 0, 0));
        assertDateFormatMatchesCpu(createTimestampType(3), "%x-%v", List.of(firstWeekYearEpochMicros, lastEpochMicros));
        assertDateFormatGpuThrows(createTimestampType(3), "%x", firstWeekYearEpochMicros - 1_000);
        assertDateFormatGpuThrows(createTimestampType(3), "%x", lastEpochMicros + 1_000);
    }

    @Test
    void testDateFormatRoundsToMillis()
    {
        // Whole milliseconds before and after 1970, and at the end of a second, minute, day and year, where rounding
        // up carries over
        List<LocalDateTime> wholeMillis = List.of(
                LocalDateTime.of(1970, 1, 1, 0, 0),
                LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_000_000),
                LocalDateTime.of(1901, 12, 31, 23, 59, 59, 999_000_000),
                LocalDateTime.of(2026, 9, 30, 12, 34, 56, 111_000_000),
                LocalDateTime.of(2026, 9, 30, 12, 34, 56, 555_000_000),
                LocalDateTime.of(2026, 9, 30, 12, 34, 59, 999_000_000),
                LocalDateTime.of(2026, 9, 30, 23, 59, 59, 999_000_000),
                LocalDateTime.of(2019, 12, 31, 23, 59, 59, 999_000_000));

        for (int precision = 4; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            List<Long> epochNanos = withSubMillisecondFractions(wholeMillis, precision);
            TimestampType timestampType = createTimestampType(precision);
            BlockBuilder builder = timestampType.createBlockBuilder(null, epochNanos.size());
            for (long nanos : epochNanos) {
                if (timestampType.isShort()) {
                    timestampType.writeLong(builder, floorDiv(nanos, NANOSECONDS_PER_MICROSECOND));
                }
                else {
                    timestampType.writeObject(builder, new LongTimestamp(
                            floorDiv(nanos, NANOSECONDS_PER_MICROSECOND),
                            floorMod(nanos, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
                }
            }
            try {
                assertGpuMatchesCpu(List.of(new Page(builder.build())), List.of(timestampType), dateFormatExpression(timestampType, "%Y-%m-%d %T.%f"));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("type: " + timestampType));
                throw t;
            }
        }

        // The same instants as timestamp with time zone, which rounds from the picoseconds of the millisecond
        for (int precision = 4; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            List<Long> epochNanos = withSubMillisecondFractions(wholeMillis, precision);
            TimestampWithTimeZoneType type = createTimestampWithTimeZoneType(precision);
            BlockBuilder builder = type.createBlockBuilder(null, epochNanos.size());
            for (long nanos : epochNanos) {
                type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        floorDiv(nanos, NANOSECONDS_PER_MILLISECOND),
                        toIntExact(floorMod(nanos, NANOSECONDS_PER_MILLISECOND) * (long) PICOSECONDS_PER_NANOSECOND),
                        UTC_KEY));
            }
            try {
                assertGpuMatchesCpu(List.of(new Page(builder.build())), List.of(type), dateFormatExpression(type, "%Y-%m-%d %T.%f"));
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("type: " + type));
                throw t;
            }
        }
    }

    /// Each of `wholeMillis` plus sub-millisecond fractions expressed in the units of `precision`: none, the smallest,
    /// just below half, exactly half, just above half and the largest, and repeated 1s and 5s (`.1111`, `.5555` at
    /// precision 4). Added to a millisecond before 1970, they give the negative counterparts, e.g.
    /// `1969-12-31 23:59:59.999` plus just below half is just past half below 1970.
    private static List<Long> withSubMillisecondFractions(List<LocalDateTime> wholeMillis, int precision)
    {
        long nanosPerUnit = LongMath.pow(10, 9 - precision);
        long halfMilliNanos = NANOSECONDS_PER_MILLISECOND / 2;
        List<Long> fractions = List.of(
                0L,
                nanosPerUnit,
                halfMilliNanos - nanosPerUnit,
                halfMilliNanos,
                halfMilliNanos + nanosPerUnit,
                NANOSECONDS_PER_MILLISECOND - nanosPerUnit,
                111_111 / nanosPerUnit * nanosPerUnit,
                555_555 / nanosPerUnit * nanosPerUnit);
        ImmutableList.Builder<Long> epochNanos = ImmutableList.builder();
        for (LocalDateTime millis : wholeMillis) {
            long millisNanos = multiplyExact(millis.toEpochSecond(UTC), NANOSECONDS_PER_SECOND) + millis.getNano();
            for (long fraction : fractions) {
                epochNanos.add(millisNanos + fraction);
            }
        }
        return epochNanos.build();
    }

    @Test
    void testDateFormatFixedValues()
    {
        TimestampType timestampType = createTimestampType(3);

        // 12-hour clock around midnight, noon, just after noon, and before 1970
        List<Long> clock = List.of(
                epochMicros(LocalDateTime.of(1969, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(1969, 12, 31, 0, 59, 59)),
                epochMicros(LocalDateTime.of(1969, 12, 31, 11, 59, 59)),
                epochMicros(LocalDateTime.of(1969, 12, 31, 12, 0)),
                epochMicros(LocalDateTime.of(1969, 12, 31, 13, 0)),
                epochMicros(LocalDateTime.of(1970, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(1970, 1, 1, 12, 0)),
                epochMicros(LocalDateTime.of(1970, 1, 1, 23, 59)));
        assertDateFormatMatchesCpu(timestampType, "%h %I %H %i %s %S %T", clock);

        // Day of year across leap-year boundaries in different centuries
        List<Long> leapDays = List.of(
                epochMicros(LocalDateTime.of(1900, 2, 28, 0, 0)),
                epochMicros(LocalDateTime.of(1900, 3, 1, 0, 0)),
                epochMicros(LocalDateTime.of(2000, 2, 29, 0, 0)),
                epochMicros(LocalDateTime.of(2000, 3, 1, 0, 0)),
                epochMicros(LocalDateTime.of(2024, 2, 29, 23, 59, 59)),
                epochMicros(LocalDateTime.of(1600, 2, 29, 0, 0)),
                epochMicros(LocalDateTime.of(1900, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(2000, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(0, 2, 29, 0, 0)),
                epochMicros(LocalDateTime.of(0, 12, 31, 0, 0)));
        assertDateFormatMatchesCpu(timestampType, "%Y-%m-%d %j", leapDays);

        // ISO week and its year where the year of the ISO week differs from the year
        List<Long> weekBoundaries = List.of(
                epochMicros(LocalDateTime.of(2020, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(2021, 1, 3, 0, 0)),
                epochMicros(LocalDateTime.of(2021, 1, 4, 0, 0)),
                epochMicros(LocalDateTime.of(2018, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(2024, 12, 29, 0, 0)),
                epochMicros(LocalDateTime.of(2024, 12, 30, 0, 0)),
                epochMicros(LocalDateTime.of(2026, 12, 31, 0, 0)),
                epochMicros(LocalDateTime.of(1969, 12, 29, 0, 0)));
        assertDateFormatMatchesCpu(timestampType, "%x-%v %Y", weekBoundaries);

        // Zero-padding at width boundaries
        List<Long> paddingYears = List.of(
                epochMicros(LocalDateTime.of(5, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(99, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(100, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(999, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(1000, 1, 1, 0, 0)),
                epochMicros(LocalDateTime.of(9999, 12, 31, 23, 59, 59)));
        assertDateFormatMatchesCpu(timestampType, "%Y %y %x %v %j %m %d", paddingYears);

        // Fraction of second, before and after 1970
        assertDateFormatMatchesCpu(timestampType, "%f", List.of(5_000L, -1_000L, 999_000L, SOME_INSTANT));

        // Common formats, non-ASCII literal text including a character outside the basic multilingual plane, and
        // characters that are not special in MySQL formats but may be in cuDF
        for (String format : List.of(
                "%Y-%m-%d",
                "%Y%m%d%H%i%s",
                "%Y-%m-%dT%T.%f",
                "%Y年%m月%d日 😀",
                "100%%",
                "%%Y",
                "%%%",
                "\\%Y",
                "'%Y'",
                "%Y\u0000")) {
            assertDateFormatMatchesCpu(timestampType, format, List.of(SOME_INSTANT));
        }
    }

    @Test
    void testDateFormatRandomFormats()
    {
        // Formats the GPU compiles must print like the CPU, and formats the CPU rejects must not compile for the GPU
        List<String> tokens = List.of(
                "%a",
                "%b",
                "%c",
                "%d",
                "%e",
                "%f",
                "%H",
                "%h",
                "%I",
                "%i",
                "%j",
                "%k",
                "%l",
                "%M",
                "%m",
                "%p",
                "%r",
                "%S",
                "%s",
                "%T",
                "%v",
                "%x",
                "%W",
                "%Y",
                "%y",
                "%w",
                "%U",
                "%u",
                "%V",
                "%X",
                "%D",
                "%%",
                "%Q",
                "%",
                "-",
                " ",
                "年",
                "😀",
                "'");
        TimestampType timestampType = createTimestampType(3);
        long before1970 = epochMicros(LocalDateTime.of(1901, 2, 3, 4, 5, 6)) + 7_000;
        Random random = new Random(11);
        for (int i = 0; i < 300; i++) {
            StringBuilder format = new StringBuilder();
            int tokenCount = random.nextInt(1, 6);
            for (int j = 0; j < tokenCount; j++) {
                format.append(tokens.get(random.nextInt(tokens.size())));
            }
            Expression expression = dateFormatExpression(timestampType, format.toString());
            boolean gpuCompiles = compileExpression(expression, layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).isPresent();
            try {
                DateFormat.format(SESSION, SOME_INSTANT, Slices.utf8Slice(format.toString()));
            }
            catch (TrinoException _) {
                assertThat(gpuCompiles)
                        .describedAs(format.toString())
                        .isFalse();
                continue;
            }
            if (gpuCompiles) {
                try {
                    assertDateFormatMatchesCpu(timestampType, format.toString(), List.of(SOME_INSTANT, before1970));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("format: " + format));
                    throw t;
                }
            }
        }
    }

    @Test
    void testDateFormatEveryAsciiSpecifier()
    {
        TimestampType timestampType = createTimestampType(3);
        List<Long> values = List.of(
                SOME_INSTANT,
                epochMicros(LocalDateTime.of(1901, 2, 3, 4, 5, 6)) + 7_000,
                epochMicros(LocalDateTime.of(2024, 12, 30, 0, 0)),
                epochMicros(LocalDateTime.of(2021, 1, 3, 12, 59, 59)) + 999_000);
        StringBuilder gpuSpecifiers = new StringBuilder();
        for (char c = 0; c < 128; c++) {
            String format = "%" + c;
            Expression expression = dateFormatExpression(timestampType, format);
            if (compileExpression(expression, layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).isEmpty()) {
                continue;
            }
            gpuSpecifiers.append(c);
            try {
                assertDateFormatMatchesCpu(timestampType, format, values);
            }
            catch (Throwable t) {
                t.addSuppressed(new Exception("format: " + format));
                throw t;
            }
        }
        assertThat(gpuSpecifiers.toString()).isEqualTo("%HISTYdfhijmsvxy");
    }

    /// The GPU representation of a timestamp with time zone holds only values in UTC, so a value in another zone
    /// fails on its way to the device rather than being formatted in UTC.
    @Test
    void testDateFormatTimestampWithTimeZoneRejectsZoneOtherThanUtc()
    {
        long instantMillis = SOME_INSTANT / 1_000;
        for (TimeZoneKey zoneKey : List.of(getTimeZoneKeyForOffset(5 * 60 + 30), getTimeZoneKey("Europe/Warsaw"), getTimeZoneKey("America/New_York"))) {
            for (TimestampWithTimeZoneType type : List.of(createTimestampWithTimeZoneType(3), createTimestampWithTimeZoneType(6))) {
                BlockBuilder builder = type.createBlockBuilder(null, 1);
                if (type.isShort()) {
                    type.writeLong(builder, packDateTimeWithZone(instantMillis, zoneKey));
                }
                else {
                    type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(instantMillis, 0, zoneKey));
                }
                Expression expression = dateFormatExpression(type, "%Y-%m-%d %H:%i:%s");
                List<Type> inputTypes = List.of(type);
                CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
                List<Page> inputPages = List.of(new Page(builder.build()));
                assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                        .hasErrorCode(NOT_SUPPORTED);
            }
        }
    }

    @Test
    void testDateFormatUnsupported()
    {
        Type timestampType = createTimestampType(3);
        // Text, locale-dependent and unpadded fields, specifiers the CPU rejects, and formats that print nothing
        for (String format : List.of("%a", "%b", "%c", "%e", "%k", "%l", "%M", "%p", "%r", "%W", "%w", "%U", "%u", "%V", "%X", "%D", "%Y %M", "%Q", "%年", "%😀", "", "%")) {
            assertThat(compileExpression(dateFormatExpression(timestampType, format), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .describedAs(format)
                    .isEmpty();
        }

        // A non-constant format
        Expression nonConstantFormat = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_format", fromTypes(timestampType, VARCHAR)),
                ImmutableList.of(field(0, timestampType), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantFormat, layoutFor(List.of(timestampType, VARCHAR)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();

        // A null format
        Expression nullFormat = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_format", fromTypes(timestampType, VARCHAR)),
                ImmutableList.of(field(0, timestampType), new Constant(VARCHAR, null)));
        assertThat(compileExpression(nullFormat, layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();

        // Precisions cuDF has no resolution for
        for (int precision = MAX_GPU_TIMESTAMP_PRECISION + 1; precision <= TimestampType.MAX_PRECISION; precision++) {
            for (Type type : List.of(createTimestampType(precision), createTimestampWithTimeZoneType(precision))) {
                assertThat(compileExpression(dateFormatExpression(type, "%Y"), layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                        .describedAs(type.toString())
                        .isEmpty();
            }
        }

        // A constant timestamp with time zone in a zone other than UTC
        TimestampWithTimeZoneType timestampWithTimeZoneType = createTimestampWithTimeZoneType(3);
        Expression constantInWarsaw = new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_format", fromTypes(timestampWithTimeZoneType, createVarcharType(2))),
                ImmutableList.of(
                        new Constant(timestampWithTimeZoneType, packDateTimeWithZone(0, getTimeZoneKey("Europe/Warsaw"))),
                        new Constant(createVarcharType(2), Slices.utf8Slice("%H"))));
        assertThat(compileExpression(constantInWarsaw, layoutFor(List.of()), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private void assertDateFormatMatchesCpu(TimestampType timestampType, String format, List<Long> epochMicros)
    {
        Page input = new Page(timestampBlock(timestampType, epochMicros));
        assertGpuMatchesCpu(List.of(input), List.of(timestampType), dateFormatExpression(timestampType, format));
    }

    private void assertDateFormatGpuThrows(TimestampType timestampType, String format, long epochMicros)
    {
        Expression expression = dateFormatExpression(timestampType, format);
        List<Page> inputPages = List.of(new Page(timestampBlock(timestampType, List.of(epochMicros))));
        List<Type> inputTypes = List.of(timestampType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Year outside of range supported by GPU for formatting");
    }

    private static Block timestampBlock(TimestampType timestampType, List<Long> epochMicros)
    {
        BlockBuilder builder = timestampType.createBlockBuilder(null, epochMicros.size());
        for (Long micros : epochMicros) {
            if (micros == null) {
                builder.appendNull();
            }
            else if (timestampType.isShort()) {
                timestampType.writeLong(builder, micros);
            }
            else {
                timestampType.writeObject(builder, new LongTimestamp(micros, 0));
            }
        }
        return builder.build();
    }

    private static long epochMicros(LocalDateTime dateTime)
    {
        return dateTime.toEpochSecond(UTC) * 1_000_000;
    }

    private static Expression dateFormatExpression(Type timestampType, String format)
    {
        Type formatType = createVarcharType(format.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("date_format", fromTypes(timestampType, formatType)),
                ImmutableList.of(field(0, timestampType), new Constant(formatType, Slices.utf8Slice(format))));
    }
}
