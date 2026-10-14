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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuUpper;
import io.trino.operator.scalar.StringFunctions;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.gpu.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.gpu.GpuTestUtils.executeWithGpu;
import static io.trino.operator.gpu.GpuTestUtils.field;
import static io.trino.operator.gpu.GpuTestUtils.layoutFor;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.varcharsBlock;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuUpper
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testUpper()
    {
        // Exercise every code point (in MAX_BATCH_SIZE pages) except those cuDF and Trino disagree on. (these are covered by testUpperRejectedCodePoints
        List<String> supportedCodePoints = IntStream.rangeClosed(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT)
                .filter(codePoint -> !GpuUpper.UNSUPPORTED_CODE_POINTS.contains(codePoint))
                .mapToObj(codePoint -> new String(Character.toChars(codePoint)))
                .collect(toImmutableList());
        List<Page> inputPages = Lists.partition(supportedCodePoints, MAX_BATCH_SIZE).stream()
                .map(codePoints -> new Page(varcharsBlock(codePoints)))
                .collect(toImmutableList());

        assertGpuMatchesCpu(inputPages, List.of(VARCHAR), createUpperExpression(VARCHAR), Set.of(0));
    }

    @Test
    void testUpperMatchesCpu()
    {
        // cuDF maps ß to "SS"; the GPU must keep Trino's ß, wherever it sits and whatever surrounds it
        assertUpperMatchesCpu(
                "ß",
                "ßß",
                "straße",
                "ßabc",
                "abcß",
                "sß",
                "SSß",
                "ss",
                "SS",
                // capital sharp S is supported as is
                "ẞ",
                "ßẞ",
                // above the first unsupported code point, so the rejection check runs and passes before the swap
                "ßαβγ",
                "ß😀",
                "😀ß😀",
                "hello",
                "æther",
                "łąka",
                "MiXeD123",
                null,
                "");
        // Þ (U+00DE) is the last code point before ß, so the batch takes the plain cuDF upper()
        assertUpperMatchesCpu("abc", "Þþ", "xyzÞ", null, "");
        // é (U+00E9) is above ß, so the batch takes the ß swap path even though it has no ß
        assertUpperMatchesCpu("é", "abc", null);
        assertUpperMatchesCpu((String) null);
        assertUpperMatchesCpu("");
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("ß", "sß", "ßß", null, "", "æther", "łąka", "abc")))),
                List.of(createCharType(8)),
                createUpperExpression(createCharType(8)),
                Set.of(0));
    }

    @Test
    void testUpperRejectedCodePoints()
    {
        for (int codePoint : GpuUpper.UNSUPPORTED_CODE_POINTS) {
            String value = Character.toString(codePoint);
            assertUpperRejected(value);
            assertUpperRejected("ab" + value + "cd");
            assertUpperRejected("straße", null, "", value + "ß");
        }

        // A real ß placeholder (ƛ, U+019B) must be rejected, not swapped to ß, whether or not a ß is in the same row or batch
        assertUpperRejected("ßƛ");
        assertUpperRejected("ƛß");
        assertUpperRejected("straße", "ƛ");
        assertUpperRejected("ƛ", null, "ß");
    }

    @Test
    void testUnsupportedUpperCodePointsDisagreeBetweenCudfAndTrino()
    {
        // Confirms GpuUpper.UNSUPPORTED_CODE_POINTS is still accurate: if cuDF ever fixes one of these mappings to
        // match Trino, this test starts failing for that code point and the entry can be pruned from the list.
        for (int codePoint : GpuUpper.UNSUPPORTED_CODE_POINTS) {
            String value = Character.toString(codePoint);
            String trinoUpper = StringFunctions.upper(Slices.utf8Slice(value)).toStringUtf8();
            assertThat(cudfUpper(value))
                    .describedAs("U+%04X", codePoint)
                    .isNotEqualTo(trinoUpper);
        }

        // cuDF uses Unicode's full case mapping and expands these to several code points, while Trino maps each
        // code point on its own
        Map<Integer, String> cudfExpansions = ImmutableMap.<Integer, String>builder()
                .put(0x0149, "ʼN") // ŉ, Trino leaves it unchanged
                .put(0x01F0, "J̌") // ǰ
                .put(0x0390, "Ϊ́") // ΐ, expands to three code points
                .put(0x0587, "ԵՒ") // Armenian ligature և
                .put(0x1E9A, "Aʾ") // ẚ
                .put(0x1F80, "ἈΙ") // ᾀ, Trino maps it to the single code point ᾈ (U+1F88)
                .put(0x1FB3, "ΑΙ") // ᾳ, Trino maps it to the single code point ᾼ (U+1FBC)
                .put(0x1FB7, "Α͂Ι") // ᾷ, expands to three code points
                .put(0xFB00, "FF") // ﬀ
                .put(0xFB03, "FFI") // ﬃ
                .put(0xFB17, "ՄԽ") // Armenian ligature ﬗ
                .buildOrThrow();
        cudfExpansions.forEach((codePoint, expansion) -> {
            assertThat(GpuUpper.UNSUPPORTED_CODE_POINTS).contains(codePoint);
            assertThat(cudfUpper(Character.toString(codePoint)))
                    .describedAs("U+%04X", codePoint)
                    .isEqualTo(expansion);
        });

        // The ß placeholder (ƛ, U+019B) is only safe because cuDF leaves it unchanged and it is unsupported
        assertThat(cudfUpper("ƛ")).isEqualTo("ƛ");
        assertThat(GpuUpper.UNSUPPORTED_CODE_POINTS).contains(0x019B);
    }

    private static Expression createUpperExpression(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("upper", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }

    private void assertUpperMatchesCpu(String... values)
    {
        assertGpuMatchesCpu(List.of(new Page(varcharsBlock(Arrays.asList(values)))), List.of(VARCHAR), createUpperExpression(VARCHAR), Set.of(0));
    }

    private void assertUpperRejected(String... values)
    {
        List<Page> inputPages = List.of(new Page(varcharsBlock(Arrays.asList(values))));
        Expression expression = createUpperExpression(VARCHAR);
        CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(VARCHAR)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
        assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(VARCHAR), expression, gpuExpression))
                .describedAs("%s", Arrays.asList(values))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("upper() on GPU does not support code points whose uppercase mapping differs between cuDF and Trino");
    }

    private static String cudfUpper(String value)
    {
        try (ColumnVector column = ColumnVector.fromStrings(value);
                ColumnVector upper = column.upper();
                HostColumnVector host = upper.copyToHost()) {
            return host.getJavaString(0);
        }
    }
}
