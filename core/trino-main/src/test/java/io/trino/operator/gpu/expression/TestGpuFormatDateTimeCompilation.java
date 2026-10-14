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
package io.trino.operator.gpu.expression;

import io.trino.operator.gpu.expression.GpuFormatDateTime.YearRange;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/// Table-tests the Joda-Time to cuDF format translation without a GPU. See
/// [io.trino.operator.gpu.TestGpuExpressions] for tests comparing the formatted values with the CPU.
class TestGpuFormatDateTimeCompilation
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("patterns")
    void testTranslate(String jodaPattern, Optional<String> cudfFormat)
    {
        assertThat(GpuFormatDateTime.translateJodaToCudfPattern(jodaPattern).map(GpuFormatDateTime.Translation::cudfFormat))
                .describedAs(jodaPattern)
                .isEqualTo(cudfFormat);
    }

    @Test
    void testYearRange()
    {
        assertYearRange("yyyy", YearRange.FOUR_DIGIT);
        assertYearRange("yy", YearRange.FOUR_DIGIT);
        assertYearRange("DDD yyyy", YearRange.FOUR_DIGIT);
        assertYearRange("yyyy DDD", YearRange.FOUR_DIGIT);
        assertYearRange("DDD", YearRange.CUDF_CALENDAR);
        assertYearRange("DDD MM-dd HH:mm:ss.SSS", YearRange.CUDF_CALENDAR);
        assertYearRange("ww", YearRange.ISO_WEEK);
        assertYearRange("DDD ww", YearRange.ISO_WEEK);
        assertYearRange("ww DDD", YearRange.ISO_WEEK);
        assertYearRange("ww yyyy", YearRange.FOUR_DIGIT);
        assertYearRange("xxxx-ww", YearRange.FOUR_DIGIT_WEEK_YEAR);
        assertYearRange("xxxx yyyy", YearRange.FOUR_DIGIT_WEEK_YEAR);
        assertYearRange("yyyy xxxx", YearRange.FOUR_DIGIT_WEEK_YEAR);
        assertYearRange("MM-dd HH:mm:ss.SSS", YearRange.UNCHECKED);
    }

    private static void assertYearRange(String jodaPattern, YearRange expected)
    {
        assertThat(GpuFormatDateTime.translateJodaToCudfPattern(jodaPattern).orElseThrow().yearRange())
                .describedAs(jodaPattern)
                .isEqualTo(expected);
    }

    private static Stream<Arguments> patterns()
    {
        return Stream.of(
                arguments("yyyy-MM-dd'T'HH:mm:ss.SSS", Optional.of("%Y-%m-%dT%H:%M:%S.%3f")),
                arguments("100%", Optional.of("100%%")),
                arguments("ss%", Optional.of("%S%%")),
                arguments("'100%'", Optional.of("100%%")),
                arguments("'%Y' yyyy", Optional.of("%%Y %Y")),
                arguments("'o''clock'", Optional.of("o'clock")),
                arguments("''", Optional.of("'")),
                arguments("'''", Optional.of("'")),
                arguments("yyyy 'abc", Optional.of("%Y abc")),
                arguments("yyyy'", Optional.of("%Y")),
                arguments("yyyy年MM月dd日", Optional.of("%Y年%m月%d日")),
                arguments("S", Optional.of("%1f")),
                arguments("SSSSSS", Optional.of("%6f")),
                arguments("SSSSSSSSS", Optional.of("%9f")),
                arguments("e", Optional.of("%u")),
                arguments("ee", Optional.of("0%u")),
                arguments("ww", Optional.of("%V")),
                arguments("xxxx", Optional.of("%G")),
                // A pattern that translates to an empty format, which cuDF rejects
                arguments("'", Optional.empty()),
                // Characters that are not special in Joda-Time but may be in cuDF
                arguments("'%y'", Optional.of("%%y")),
                arguments("'%f'", Optional.of("%%f")),
                arguments("\"yyyy\"", Optional.of("\"%Y\"")),
                arguments("'\"'", Optional.of("\"")),
                arguments("yyyy\\MM", Optional.of("%Y\\%m")),
                arguments("'\\\\'", Optional.of("\\\\")),
                arguments("yyyy\u0000", Optional.of("%Y\u0000")),
                arguments("'\t'", Optional.of("\t")),
                // Joda-Time rejects an empty pattern
                arguments("", Optional.empty()),
                // Unpadded, text, locale-dependent and time zone fields, and lookalikes that must not be mapped
                arguments("M", Optional.empty()),
                arguments("d", Optional.empty()),
                arguments("yyy", Optional.empty()),
                arguments("yyyyy", Optional.empty()),
                arguments("YYYY", Optional.empty()),
                arguments("xx", Optional.empty()),
                arguments("MMM", Optional.empty()),
                arguments("EEE", Optional.empty()),
                arguments("a", Optional.empty()),
                arguments("Z", Optional.empty()),
                arguments("z", Optional.empty()),
                arguments("q", Optional.empty()),
                arguments("T", Optional.empty()),
                arguments("K", Optional.empty()),
                arguments("k", Optional.empty()),
                arguments("DD", Optional.empty()),
                arguments("ddd", Optional.empty()),
                arguments("SSSSSSSSSS", Optional.empty()),
                arguments("eee", Optional.empty()),
                arguments("w", Optional.empty()));
    }
}
