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
package io.trino.operator.torin;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.StringFunctions;
import io.trino.operator.torin.expression.CompiledExpression;
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
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.torin.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.torin.GpuTestUtils.TEST_SESSION_TIME_ZONE_KEY;
import static io.trino.operator.torin.GpuTestUtils.assertGpuMatchesCpu;
import static io.trino.operator.torin.GpuTestUtils.executeWithGpu;
import static io.trino.operator.torin.GpuTestUtils.field;
import static io.trino.operator.torin.GpuTestUtils.layoutFor;
import static io.trino.operator.torin.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.torin.GpuTestUtils.varcharsBlock;
import static io.trino.operator.torin.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.operator.torin.expression.GpuLower.UNSUPPORTED_CODE_POINTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestGpuLower
{
    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testLower()
    {
        // Exercise every code point (in MAX_BATCH_SIZE pages) except those cuDF and Trino disagree on.
        List<String> supportedCodePoints = IntStream.rangeClosed(Character.MIN_CODE_POINT, Character.MAX_CODE_POINT)
                .filter(codePoint -> !UNSUPPORTED_CODE_POINTS.contains(codePoint))
                .mapToObj(codePoint -> new String(Character.toChars(codePoint)))
                .collect(toImmutableList());
        List<Page> inputPages = Lists.partition(supportedCodePoints, MAX_BATCH_SIZE).stream()
                .map(codePoints -> new Page(varcharsBlock(codePoints)))
                .collect(toImmutableList());

        assertGpuMatchesCpu(inputPages, List.of(VARCHAR), createLowerExpression(VARCHAR));
    }

    @Test
    void testLowerRejectedCodePoints()
    {
        Type type = VARCHAR;
        for (int codePoint : UNSUPPORTED_CODE_POINTS) {
            List<Page> inputPages = List.of(new Page(writeNativeValue(type, Slices.utf8Slice(new String(Character.toChars(codePoint))))));
            Expression expression = createLowerExpression(type);
            CompiledExpression gpuExpression = compileExpression(expression, layoutFor(List.of(type)), SQL_STANDARD, TEST_SESSION_TIME_ZONE_KEY)
                    .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + expression));
            assertTrinoExceptionThrownBy(() -> executeWithGpu(inputPages, List.of(type), expression, gpuExpression))
                    .describedAs("U+%04X", codePoint)
                    .hasErrorCode(NOT_SUPPORTED)
                    .hasMessage("lower() on GPU does not support code points whose lowercase mapping differs between cuDF and Trino");
        }
    }

    @Test
    void testUnsupportedCodePointsDisagreeBetweenCudfAndTrino()
    {
        // Confirms UNSUPPORTED_CODE_POINTS is still accurate: if cuDF ever fixes one of these mappings to
        // match Trino, this test starts failing for that code point and the entry can be pruned from the list.
        for (int codePoint : UNSUPPORTED_CODE_POINTS) {
            String value = new String(Character.toChars(codePoint));
            String trinoLower = StringFunctions.lower(Slices.utf8Slice(value)).toStringUtf8();
            try (ColumnVector column = ColumnVector.fromStrings(value);
                    ColumnVector lowered = column.lower();
                    HostColumnVector host = lowered.copyToHost()) {
                assertThat(host.getJavaString(0))
                        .describedAs("U+%04X", codePoint)
                        .isNotEqualTo(trinoLower);
            }
        }
    }

    @Test
    void testLowerReconcilesAndHandlesNulls()
    {
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("HELLO", "Æther", "İstanbul", null, "ŁĄKA", "", "MiXeD123")))),
                List.of(VARCHAR),
                createLowerExpression(VARCHAR));
        assertGpuMatchesCpu(
                List.of(new Page(varcharsBlock(Arrays.asList("HELLO", "Æther", "İ", null, "ŁĄKA", "", "ABC")))),
                List.of(createCharType(8)),
                createLowerExpression(createCharType(8)));
    }

    private static Expression createLowerExpression(Type type)
    {
        return new Call(
                FUNCTION_RESOLUTION.resolveFunction("lower", fromTypes(type)),
                ImmutableList.of(field(0, type)));
    }
}
