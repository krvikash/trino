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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.CaseMappingUtils.charUtf8;
import static io.trino.operator.gpu.expression.CaseMappingUtils.codePointRange;
import static io.trino.operator.gpu.expression.CaseMappingUtils.failOnRejectedCodePoints;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class TestCaseMappingUtils
{
    private static final String REJECTED_MESSAGE = "rejected";

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testCharUtf8()
    {
        // one UTF-8 byte per width, plus the extremes
        assertThat(charUtf8(0x0000)).isEqualTo(0x00);
        assertThat(charUtf8('A')).isEqualTo(0x41);
        assertThat(charUtf8(0x007F)).isEqualTo(0x7F);
        assertThat(charUtf8(0x00DF)).isEqualTo(0xC39F);
        assertThat(charUtf8(0x07FF)).isEqualTo(0xDFBF);
        assertThat(charUtf8(0x1C89)).isEqualTo(0xE1B289);
        assertThat(charUtf8(0xFFFF)).isEqualTo(0xEFBFBF);
        // four-byte sequences pack into negative ints
        assertThat(charUtf8(0x10400)).isEqualTo(0xF0909080);
        assertThat(charUtf8(Character.MAX_CODE_POINT)).isEqualTo(0xF48FBFBF);
    }

    @Test
    void testCharUtf8MatchesCudfCodePoints()
    {
        List<Integer> codePoints = ImmutableList.of(0x0001, 0x0041, 0x007F, 0x0080, 0x00DF, 0x019B, 0x07FF, 0x0800, 0x1C89, 0xFFFF, 0x10000, 0x10400, 0x1E943, Character.MAX_CODE_POINT);
        String[] values = codePoints.stream()
                .map(Character::toString)
                .toArray(String[]::new);
        try (ColumnVector column = ColumnVector.fromStrings(values);
                ColumnVector characters = column.codePoints();
                HostColumnVector host = characters.copyToHost()) {
            for (int i = 0; i < codePoints.size(); i++) {
                assertThat(charUtf8(codePoints.get(i)))
                        .describedAs("U+%04X", codePoints.get(i))
                        .isEqualTo(host.getInt(i));
            }
        }
    }

    @Test
    void testMaxCharUtf8()
    {
        assertThat(maxCharUtf8(new String[0])).isEqualTo(0);
        assertThat(maxCharUtf8(null, null)).isEqualTo(0);
        assertThat(maxCharUtf8("", "")).isEqualTo(0);
        assertThat(maxCharUtf8("abc", null, "")).isEqualTo(charUtf8('c'));
        assertThat(maxCharUtf8("a", "ß", "b")).isEqualTo(charUtf8(0x00DF));
        // supplementary characters are negative as signed ints, but must still order as the largest
        assertThat(maxCharUtf8("ÿ", "𐐀", "￿")).isEqualTo(charUtf8(0x10400));
        assertThat(maxCharUtf8("a" + Character.toString(Character.MAX_CODE_POINT) + "b")).isEqualTo(charUtf8(Character.MAX_CODE_POINT));
    }

    @Test
    void testFailOnRejectedCodePoints()
    {
        int[] rejected = {charUtf8(0x019B), charUtf8(0x10428)};

        assertNotRejected(rejected);
        assertNotRejected(rejected, null, null);
        assertNotRejected(rejected, "", "abc", "ß", "Ƛ", "𐐀");

        assertRejected(rejected, "ƛ");
        assertRejected(rejected, "abc", null, "xƛy");
        assertRejected(rejected, "ok", "𐐨");
        assertRejected(rejected, "ƛ𐐨");
    }

    @Test
    void testCodePointRange()
    {
        assertThat(ImmutableList.copyOf(codePointRange(0x1E96, 0x1E9A))).containsExactly(0x1E96, 0x1E97, 0x1E98, 0x1E99, 0x1E9A);
        assertThat(ImmutableList.copyOf(codePointRange(0x00DF, 0x00DF))).containsExactly(0x00DF);
        assertThat(ImmutableList.copyOf(codePointRange(0x00E0, 0x00DF))).isEmpty();
    }

    private static int maxCharUtf8(String... values)
    {
        try (ColumnVector column = ColumnVector.fromStrings(values)) {
            return CaseMappingUtils.maxCharUtf8(column);
        }
    }

    private static void assertNotRejected(int[] rejected, String... values)
    {
        try (ColumnVector column = ColumnVector.fromStrings(values)) {
            assertThatNoException().isThrownBy(() -> failOnRejectedCodePoints(column, rejected, REJECTED_MESSAGE));
        }
    }

    private static void assertRejected(int[] rejected, String... values)
    {
        try (ColumnVector column = ColumnVector.fromStrings(values)) {
            assertTrinoExceptionThrownBy(() -> failOnRejectedCodePoints(column, rejected, REJECTED_MESSAGE))
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage(REJECTED_MESSAGE);
        }
    }
}
