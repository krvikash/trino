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
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.base.VerifyException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestGpuBindings
{
    @BeforeAll
    static void setup()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testLetRemovesBindingAfterSuccessOrFailure()
    {
        try (ColumnVector outer = ColumnVector.fromLongs(11)) {
            Bindings bindings = new Bindings();
            bindings.put(7, outer);
            GpuExpression expression = new GpuLet(19, constant(22L), new GpuBoundReference(7));
            try (ColumnVector result = expression.evaluate(1, List.of(), bindings)) {
                assertValues(result, 11);
            }
            assertThat(bindings.allBindings()).extracting(Entry::getKey).containsExactly(7);
            assertThat(bindings.binding(7)).isSameAs(outer);

            GpuExpression failing = new GpuLet(19, constant(22L), new GpuBoundReference(99));
            assertThatThrownBy(() -> failing.evaluate(1, List.of(), bindings))
                    .isInstanceOf(VerifyException.class)
                    .hasMessage("Unknown binding: 99");
            assertThat(bindings.allBindings()).extracting(Entry::getKey).containsExactly(7);
            assertThat(bindings.binding(7)).isSameAs(outer);
        }
    }

    @Test
    void testLetDoesNotOverwriteBinding()
    {
        try (ColumnVector outer = ColumnVector.fromLongs(11)) {
            Bindings bindings = new Bindings();
            bindings.put(7, outer);
            GpuExpression expression = new GpuLet(7, constant(22L), new GpuBoundReference(7));
            assertThatThrownBy(() -> expression.evaluate(1, List.of(), bindings))
                    .isInstanceOf(VerifyException.class)
                    .hasMessage("Binding already exists: 7");
            assertThat(bindings.allBindings()).extracting(Entry::getKey).containsExactly(7);
            assertThat(bindings.binding(7)).isSameAs(outer);
        }
    }

    @Test
    void testInsertedLetPreservesReferencesAndInputChannels()
    {
        GpuExpression body = new GpuIntegerAdd(new GpuBoundReference(7), new GetColumn(0), DType.INT64, "bigint");
        GpuExpression expression = new GpuLet(7, constant(10L), new GpuLet(19, constant(99L), body));
        try (ColumnVector input = ColumnVector.fromLongs(1, 2, 3);
                ColumnVector result = expression.evaluate(3, List.of(input), new Bindings())) {
            assertValues(result, 11, 12, 13);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIfTransformsBindings(boolean filtered)
    {
        try (ColumnVector condition = ColumnVector.fromBooleans(true, false, true, false);
                ColumnVector divisor = ColumnVector.fromLongs(2, 0, 5, 0)) {
            GpuExpression division = new GpuIntegerDivide(constant(10L), new GpuBoundReference(7), DType.INT64, "bigint");
            if (filtered) {
                // Exercise a local binding as well as inherited bindings.
                division = new GpuLet(19, constant(0L), division);
            }
            GpuExpression expression = new GpuLet(
                    7,
                    new GpuConstantColumn(divisor),
                    new GpuIf(new GpuConstantColumn(condition), division, constant(42L), BIGINT));
            // There are no external input columns: only the binding needs filtering or NULL masking.
            try (ColumnVector result = expression.evaluate(4, List.of(), new Bindings())) {
                assertValues(result, 5, 42, 2, 42);
            }
        }
    }

    @Test
    void testIfWithNestedBoundColumn()
    {
        try (ColumnVector condition = ColumnVector.fromBooleans(true, false, true, false);
                ColumnVector divisor = ColumnVector.fromLongs(2, 0, 5, 0)) {
            GpuExpression division = new GpuIntegerDivide(constant(10L), new GpuBoundReference(7), DType.INT64, "bigint");
            GpuExpression expression = new GpuLet(7, new GpuConstantColumn(divisor),
                    new GpuLet(
                            19,
                            new GpuStruct(List.of(new GpuBoundReference(7))),
                            new GpuIf(new GpuConstantColumn(condition), division, constant(42L), BIGINT)));
            try (ColumnVector result = expression.evaluate(4, List.of(), new Bindings())) {
                assertValues(result, 5, 42, 2, 42);
            }
        }
    }

    @Test
    void testCaseTransformsBindingsForRemainingAndMatchedRows()
    {
        try (ColumnVector first = ColumnVector.fromBooleans(true, false, false, false);
                ColumnVector second = ColumnVector.fromBooleans(false, true, false, true);
                ColumnVector values = ColumnVector.fromLongs(10, 20, 30, 40)) {
            GpuExpression expression = new GpuLet(7, new GpuConstantColumn(values),
                    new GpuLet(19, new GpuConstantColumn(first),
                            new GpuLet(23, new GpuConstantColumn(second),
                                    new GpuCase(
                                            List.of(new GpuBoundReference(19), new GpuBoundReference(23), new GpuIsNull(constant(null))),
                                            List.of(new GpuBoundReference(7), new GpuBoundReference(7), constant(99L)),
                                            BIGINT))));
            try (ColumnVector result = expression.evaluate(4, List.of(), new Bindings())) {
                assertValues(result, 10, 20, 99, 40);
            }
        }
    }

    private static GpuExpression constant(Long value)
    {
        return new GpuConstant(BIGINT, Optional.ofNullable(value));
    }

    private static void assertValues(ColumnVector result, long... expected)
    {
        try (HostColumnVector host = result.copyToHost()) {
            assertThat(host.getRowCount()).isEqualTo(expected.length);
            for (int i = 0; i < expected.length; i++) {
                assertThat(host.isNull(i)).isFalse();
                assertThat(host.getLong(i)).isEqualTo(expected[i]);
            }
        }
    }
}
