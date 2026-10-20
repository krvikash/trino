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
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.scalar.timestamp.FormatDateTime;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
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
import java.util.stream.IntStream;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.MAX_GPU_TIMESTAMP_PRECISION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
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
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuFormatDateTime
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testFormatDateTime(NullsProvider nullsProvider)
    {
        // Without a year field every value is formattable, so the whole range is covered
        for (String pattern : List.of("MM-dd HH:mm:ss.SSS", "'at' hh 'o''clock', 100% [ss]")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                Type timestampType = createTimestampType(precision);
                try {
                    List<Page> inputPages = List.of(new Page(64, createBlock(timestampType, 64, nullsProvider)));
                    assertGpuMatchesCpu(inputPages, List.of(timestampType), formatDateTimeExpression(timestampType, pattern));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeYear()
    {
        Random random = new Random(7);
        for (String pattern : List.of("yyyy-MM-dd HH:mm:ss.SSS", "yy/MM/dd", "DDD", "''yyyy''")) {
            for (int precision = 0; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
                TimestampType timestampType = createTimestampType(precision);
                // Precisions above 6 are nanoseconds on the GPU, which span only years 1677 to 2262
                int firstYear = timestampType.isShort() ? 0 : 1678;
                int lastYear = timestampType.isShort() ? 9999 : 2261;
                long minEpochMicros = LocalDateTime.of(firstYear, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
                long maxEpochMicros = LocalDateTime.of(lastYear + 1, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
                List<Long> epochMicros = new ArrayList<>();
                for (int i = 0; i < 64; i++) {
                    long micros = random.nextLong(minEpochMicros, maxEpochMicros);
                    epochMicros.add(i % 5 == 0 ? null : Timestamps.round(micros, max(0, 6 - precision)));
                }
                try {
                    assertFormatDateTimeMatchesCpu(timestampType, pattern, epochMicros);
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    t.addSuppressed(new Exception("precision: " + precision));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeYearOutOfRange()
    {
        String pattern = "yyyy-MM-dd HH:mm:ss.SSS";
        long firstEpochMicros = LocalDateTime.of(0, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastEpochMicros = LocalDateTime.of(10000, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        for (TimestampType timestampType : List.of(createTimestampType(3), createTimestampType(6))) {
            assertFormatDateTimeMatchesCpu(timestampType, pattern, List.of(firstEpochMicros, lastEpochMicros));
            assertFormatDateTimeGpuThrows(timestampType, pattern, firstEpochMicros - 1_000);
            assertFormatDateTimeGpuThrows(timestampType, pattern, lastEpochMicros + 1_000);
            assertFormatDateTimeGpuThrows(timestampType, "yy", firstEpochMicros - 1_000);
            assertFormatDateTimeMatchesCpu(timestampType, "DDD", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        }
        // Rounding to milliseconds carries into year 10000
        assertFormatDateTimeGpuThrows(createTimestampType(6), pattern, lastEpochMicros + 500);
        // Fields that do not depend on the year are formattable in any year
        assertFormatDateTimeMatchesCpu(createTimestampType(6), "MM-dd", List.of(firstEpochMicros - 1_000, lastEpochMicros + 1_000));
        // Day of year is computed from cuDF's calendar year
        long firstCalendarEpochMicros = LocalDateTime.of(-32767, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastCalendarEpochMicros = LocalDateTime.of(32768, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "DDD", List.of(firstCalendarEpochMicros, lastCalendarEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "DDD", firstCalendarEpochMicros - 1_000, "Year out of range supported by GPU: must be in [-32767, 32767]");
        assertFormatDateTimeGpuThrows(createTimestampType(3), "DDD", lastCalendarEpochMicros + 1_000, "Year out of range supported by GPU: must be in [-32767, 32767]");
        // ISO week is computed from cuDF's calendar year and its neighbors, so the last calendar year overflows
        long firstWeekEpochMicros = LocalDateTime.of(-32767, 1, 7, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastWeekEpochMicros = LocalDateTime.of(32767, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000 - 1_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "ww", List.of(firstCalendarEpochMicros, firstWeekEpochMicros, lastWeekEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "ww", firstCalendarEpochMicros - 1_000);
        assertFormatDateTimeGpuThrows(createTimestampType(3), "ww", lastWeekEpochMicros + 1_000);
        // The year of the ISO week is -1 in the first days of year 0
        long firstWeekYearEpochMicros = LocalDateTime.of(1, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "xxxx-ww", List.of(firstWeekYearEpochMicros, lastEpochMicros));
        assertFormatDateTimeGpuThrows(createTimestampType(3), "xxxx", firstWeekYearEpochMicros - 1_000);
        assertFormatDateTimeGpuThrows(createTimestampType(3), "xxxx", lastEpochMicros + 1_000);
    }

    private void assertFormatDateTimeMatchesCpu(TimestampType timestampType, String pattern, List<Long> epochMicros)
    {
        Page input = new Page(timestampBlock(timestampType, epochMicros));
        assertGpuMatchesCpu(List.of(input), List.of(timestampType), formatDateTimeExpression(timestampType, pattern));
    }

    private void assertFormatDateTimeGpuThrows(TimestampType timestampType, String pattern, long epochMicros)
    {
        assertFormatDateTimeGpuThrows(timestampType, pattern, epochMicros, "Year outside of range supported by GPU for formatting");
    }

    private void assertFormatDateTimeGpuThrows(TimestampType timestampType, String pattern, long epochMicros, String expectedMessage)
    {
        Expression expression = formatDateTimeExpression(timestampType, pattern);
        List<Page> inputPages = List.of(new Page(timestampBlock(timestampType, List.of(epochMicros))));
        List<Type> inputTypes = List.of(timestampType);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(inputTypes), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).orElseThrow();
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, inputTypes, expression, gpuExpression))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage(expectedMessage);
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

    @Test
    void testFormatDateTimeRoundsToMillis()
    {
        // Values near millisecond, second and year boundaries, including negative ones, where rounding carries over
        long[] epochNanos = {
                0,
                499_999,
                500_000,
                -500_000,
                -500_001,
                999_999_999,
                -1,
                1_577_836_799_999_500_000L,
                -1_000_000_000_000_000_000L,
        };
        for (int precision = 4; precision <= MAX_GPU_TIMESTAMP_PRECISION; precision++) {
            TimestampType timestampType = createTimestampType(precision);
            BlockBuilder builder = timestampType.createBlockBuilder(null, epochNanos.length);
            for (long nanos : epochNanos) {
                long aligned = Timestamps.round(nanos, 9 - precision);
                if (timestampType.isShort()) {
                    timestampType.writeLong(builder, floorDiv(aligned, NANOSECONDS_PER_MICROSECOND));
                }
                else {
                    timestampType.writeObject(builder, new LongTimestamp(
                            floorDiv(aligned, NANOSECONDS_PER_MICROSECOND),
                            floorMod(aligned, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND));
                }
            }
            Page input = new Page(builder.build());
            assertGpuMatchesCpu(List.of(input), List.of(timestampType), formatDateTimeExpression(timestampType, "yyyy-MM-dd HH:mm:ss.SSS"));
        }
    }

    @Test
    void testFormatDateTimeFixedValues()
    {
        TimestampType timestampType = createTimestampType(3);

        // hh (12-hour clock) around midnight, noon, just after noon, and before 1970
        long midnight = LocalDateTime.of(1969, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long noon = LocalDateTime.of(1969, 12, 31, 12, 0).toEpochSecond(UTC) * 1_000_000;
        long justAfterNoon = LocalDateTime.of(1969, 12, 31, 13, 0).toEpochSecond(UTC) * 1_000_000;
        long lastMinuteOfDay = LocalDateTime.of(1970, 1, 1, 23, 59).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(timestampType, "hh", List.of(midnight, noon, justAfterNoon, lastMinuteOfDay));

        // DDD (day of year) across leap-year boundaries in different centuries
        long dayBeforeLeapDay1900 = LocalDateTime.of(1900, 2, 28, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDay1900 = LocalDateTime.of(1900, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long leapDay2000 = LocalDateTime.of(2000, 2, 29, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDay2000 = LocalDateTime.of(2000, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long leapDayYear0 = LocalDateTime.of(0, 2, 29, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long dayAfterLeapDayYear0 = LocalDateTime.of(0, 3, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDay1900 = LocalDateTime.of(1900, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDay2000 = LocalDateTime.of(2000, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        long lastDayYear0 = LocalDateTime.of(0, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(
                timestampType,
                "DDD",
                List.of(dayBeforeLeapDay1900, dayAfterLeapDay1900, leapDay2000, dayAfterLeapDay2000, leapDayYear0, dayAfterLeapDayYear0, lastDay1900, lastDay2000, lastDayYear0));

        // ww (ISO week) and xxxx (its year) where the year of the ISO week differs from the year
        List<Long> weekBoundaries = List.of(
                LocalDateTime.of(2020, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2021, 1, 3, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2021, 1, 4, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2018, 12, 31, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2024, 12, 29, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(2024, 12, 30, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(1969, 12, 29, 0, 0).toEpochSecond(UTC) * 1_000_000);
        assertFormatDateTimeMatchesCpu(timestampType, "xxxx-ww", weekBoundaries);
        // In year 0, which starts in the last ISO week of year -1
        long firstDayYear0 = LocalDateTime.of(0, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000;
        assertFormatDateTimeMatchesCpu(timestampType, "ww", List.of(firstDayYear0, lastDayYear0));

        // Zero-padding at width boundaries, with both yyyy and yy
        List<Long> paddingYears = List.of(
                LocalDateTime.of(5, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(99, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(100, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(999, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(1000, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000,
                LocalDateTime.of(9999, 1, 1, 0, 0).toEpochSecond(UTC) * 1_000_000);
        assertFormatDateTimeMatchesCpu(timestampType, "yyyy", paddingYears);
        assertFormatDateTimeMatchesCpu(timestampType, "yy", paddingYears);
        assertFormatDateTimeMatchesCpu(timestampType, "xxxx", paddingYears);

        long someInstant = LocalDateTime.of(2026, 9, 30, 1, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        // A common pattern with no separators between fields
        assertFormatDateTimeMatchesCpu(timestampType, "yyyyMMddHHmmssSSS", List.of(someInstant));
        // Non-ASCII literal text, including a character outside the basic multilingual plane
        assertFormatDateTimeMatchesCpu(timestampType, "yyyy年MM月dd日 😀", List.of(someInstant));
        // Quoted text, unterminated quotes, and characters that are not special in Joda-Time but may be in cuDF
        for (String pattern : List.of(
                "'%Y' yyyy",
                "ss%",
                "'100%'",
                "'%y'",
                "'%f'",
                "yyyy 'abc",
                "yyyy'",
                "'o''clock' HH",
                "\"yyyy\"",
                "'\"'",
                "yyyy\\MM",
                "'\\\\'",
                "yyyy\u0000",
                "'\t'")) {
            assertFormatDateTimeMatchesCpu(timestampType, pattern, List.of(someInstant));
        }

        // Fraction of second of every width, truncated rather than rounded, also before 1970
        long fiveMillis = 5_000;
        long millisecondBefore1970 = -1_000;
        long lastMillisecondOfSecond = 999_000;
        for (int digits = 1; digits <= 9; digits++) {
            assertFormatDateTimeMatchesCpu(timestampType, "S".repeat(digits), List.of(fiveMillis, millisecondBefore1970, lastMillisecondOfSecond, someInstant));
        }
    }

    @Test
    void testFormatDateTimeAllCharacters()
    {
        // Every character other than a quote and the ASCII letters, which Joda-Time reads as fields, is literal text
        String pattern = IntStream.range(0, 1024)
                .filter(codePoint -> codePoint != '\'' && !(codePoint >= 'A' && codePoint <= 'Z') && !(codePoint >= 'a' && codePoint <= 'z'))
                .mapToObj(Character::toString)
                .collect(joining());
        long someInstant = LocalDateTime.of(2026, 9, 30, 13, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        assertFormatDateTimeMatchesCpu(createTimestampType(3), pattern, List.of(someInstant));
        assertFormatDateTimeMatchesCpu(createTimestampType(3), "'" + pattern + "'", List.of(someInstant));
    }

    @Test
    void testFormatDateTimeRandomPatterns()
    {
        // Patterns the GPU compiles must print like the CPU, and patterns the CPU rejects must not compile for the GPU
        List<String> tokens = List.of(
                "yyyy",
                "yy",
                "MM",
                "dd",
                "DDD",
                "HH",
                "hh",
                "mm",
                "ss",
                "S",
                "SSS",
                "SSSSSS",
                "SSSSSSSSS",
                "ee",
                "ww",
                "xxxx",
                "Y",
                "x",
                "w",
                "e",
                "K",
                "k",
                "C",
                "G",
                "E",
                "a",
                "z",
                "Z",
                "T",
                "M",
                "y",
                "'",
                "''",
                "'abc'",
                "%",
                "-",
                " ",
                "年",
                "😀");
        TimestampType timestampType = createTimestampType(3);
        long someInstant = LocalDateTime.of(2026, 9, 30, 13, 2, 3).toEpochSecond(UTC) * 1_000_000 + 456_000;
        long before1970 = LocalDateTime.of(1901, 2, 3, 4, 5, 6).toEpochSecond(UTC) * 1_000_000 + 7_000;
        Random random = new Random(11);
        for (int i = 0; i < 300; i++) {
            StringBuilder pattern = new StringBuilder();
            int tokenCount = random.nextInt(1, 6);
            for (int j = 0; j < tokenCount; j++) {
                pattern.append(tokens.get(random.nextInt(tokens.size())));
            }
            Expression expression = formatDateTimeExpression(timestampType, pattern.toString());
            boolean gpuCompiles = compileExpression(expression, layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY).isPresent();
            try {
                FormatDateTime.format(SESSION, someInstant, Slices.utf8Slice(pattern.toString()));
            }
            catch (TrinoException _) {
                assertThat(gpuCompiles)
                        .describedAs(pattern.toString())
                        .isFalse();
                continue;
            }
            if (gpuCompiles) {
                try {
                    assertFormatDateTimeMatchesCpu(timestampType, pattern.toString(), List.of(someInstant, before1970));
                }
                catch (Throwable t) {
                    t.addSuppressed(new Exception("pattern: " + pattern));
                    throw t;
                }
            }
        }
    }

    @Test
    void testFormatDateTimeDayOfWeek()
    {
        // Midnight, the last second before noon, and noon on each day of a week, before and after 1970
        List<Long> epochMicros = new ArrayList<>();
        for (LocalDateTime day : List.of(LocalDateTime.of(2026, 9, 28, 0, 0), LocalDateTime.of(1901, 2, 4, 0, 0))) {
            for (int dayOffset = 0; dayOffset < 7; dayOffset++) {
                long dayEpochMicros = day.plusDays(dayOffset).toEpochSecond(UTC) * 1_000_000;
                epochMicros.add(dayEpochMicros);
                epochMicros.add(dayEpochMicros + 12 * 3_600_000_000L - 1_000_000);
                epochMicros.add(dayEpochMicros + 12 * 3_600_000_000L);
            }
        }
        for (int precision : List.of(0, 3, 6, 9)) {
            assertFormatDateTimeMatchesCpu(createTimestampType(precision), "HH:mm:ss e ee", epochMicros);
        }
    }

    @Test
    void testFormatDateTimeUnsupportedPattern()
    {
        Type timestampType = createTimestampType(3);
        // Fields with an unsupported width, text/locale/time-zone fields, lookalikes of supported fields, patterns
        // Joda-Time rejects, a pattern that translates to an empty cuDF format, and a non-constant pattern
        for (String pattern : List.of(
                "M",
                "d",
                "yyy",
                "yyyyy",
                "DD",
                "ddd",
                "SSSSSSSSSS",
                "MMM",
                "EEE",
                "a",
                "Z",
                "z",
                "q",
                "T",
                "YYYY",
                "xx",
                "K",
                "k",
                "eee",
                "w",
                "",
                "'")) {
            assertThat(compileExpression(formatDateTimeExpression(timestampType, pattern), layoutFor(List.of(timestampType)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY))
                    .describedAs(pattern)
                    .isEmpty();
        }
        Expression nonConstantPattern = new Call(
                FUNCTION_RESOLUTION.resolveFunction("format_datetime", fromTypes(timestampType, VARCHAR)),
                ImmutableList.of(field(0, timestampType), field(1, VARCHAR)));
        assertThat(compileExpression(nonConstantPattern, layoutFor(List.of(timestampType, VARCHAR)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)).isEmpty();
    }

    private static Expression formatDateTimeExpression(Type timestampType, String pattern)
    {
        Type patternType = createVarcharType(pattern.length());
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("format_datetime", fromTypes(timestampType, patternType)),
                ImmutableList.of(field(0, timestampType), new Constant(patternType, Slices.utf8Slice(pattern))));
    }
}
