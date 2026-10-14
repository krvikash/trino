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

/// Table-tests the MySQL to cuDF format translation without a GPU. See
/// [io.trino.operator.gpu.TestGpuDateFormat] for tests comparing the formatted values with the CPU.
final class TestGpuDateFormatCompilation
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @MethodSource("formats")
    void testTranslate(String mySqlFormat, Optional<String> cudfFormat)
    {
        assertThat(GpuDateFormat.translateMySqlToCudfFormat(mySqlFormat).map(GpuFormatDateTime.Translation::cudfFormat))
                .describedAs(mySqlFormat)
                .isEqualTo(cudfFormat);
    }

    @Test
    void testYearRange()
    {
        assertYearRange("%Y", YearRange.FOUR_DIGIT);
        assertYearRange("%y", YearRange.FOUR_DIGIT);
        assertYearRange("%j %Y", YearRange.FOUR_DIGIT);
        assertYearRange("%Y %j", YearRange.FOUR_DIGIT);
        assertYearRange("%j", YearRange.CUDF_CALENDAR);
        assertYearRange("%v", YearRange.ISO_WEEK);
        assertYearRange("%v %j", YearRange.ISO_WEEK);
        assertYearRange("%v %Y", YearRange.FOUR_DIGIT);
        assertYearRange("%x-%v", YearRange.FOUR_DIGIT_WEEK_YEAR);
        assertYearRange("%Y %x", YearRange.FOUR_DIGIT_WEEK_YEAR);
        assertYearRange("%m-%d %H:%i:%s.%f", YearRange.UNCHECKED);
        assertYearRange("%%Y", YearRange.UNCHECKED);
    }

    private static void assertYearRange(String mySqlFormat, YearRange expected)
    {
        assertThat(GpuDateFormat.translateMySqlToCudfFormat(mySqlFormat).orElseThrow().yearRange())
                .describedAs(mySqlFormat)
                .isEqualTo(expected);
    }

    private static Stream<Arguments> formats()
    {
        return Stream.of(
                arguments("%Y-%m-%d %H:%i:%s.%f", Optional.of("%Y-%m-%d %H:%M:%S.%6f")),
                arguments("%y%j", Optional.of("%y%j")),
                arguments("%h %I", Optional.of("%I %I")),
                arguments("%S %s", Optional.of("%S %S")),
                arguments("%T", Optional.of("%H:%M:%S")),
                arguments("%x-%v", Optional.of("%G-%V")),
                // Literal text, and characters that are special in cuDF but not in MySQL formats
                arguments("abc", Optional.of("abc")),
                arguments("%%", Optional.of("%%")),
                arguments("100%%", Optional.of("100%%")),
                arguments("%%Y", Optional.of("%%Y")),
                arguments("%Y年%m月", Optional.of("%Y年%m月")),
                arguments("%Y%", Optional.of("%Y")),
                arguments("'%Y'", Optional.of("'%Y'")),
                arguments("\\%Y", Optional.of("\\%Y")),
                // Formats that print nothing, which the CPU rejects
                arguments("", Optional.empty()),
                arguments("%", Optional.empty()),
                // Text, locale-dependent and unpadded fields
                arguments("%a", Optional.empty()),
                arguments("%b", Optional.empty()),
                arguments("%M", Optional.empty()),
                arguments("%W", Optional.empty()),
                arguments("%p", Optional.empty()),
                arguments("%r", Optional.empty()),
                arguments("%c", Optional.empty()),
                arguments("%e", Optional.empty()),
                arguments("%k", Optional.empty()),
                arguments("%l", Optional.empty()),
                arguments("%Y-%c", Optional.empty()),
                // Specifiers the CPU rejects
                arguments("%w", Optional.empty()),
                arguments("%U", Optional.empty()),
                arguments("%u", Optional.empty()),
                arguments("%V", Optional.empty()),
                arguments("%X", Optional.empty()),
                arguments("%D", Optional.empty()),
                // Characters that are not specifiers today, as they may become ones on the CPU
                arguments("%Q", Optional.empty()),
                arguments("%z %Z %n %t", Optional.empty()),
                arguments("%年", Optional.empty()));
    }
}
