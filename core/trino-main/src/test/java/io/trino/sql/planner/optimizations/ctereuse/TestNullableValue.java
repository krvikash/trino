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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.planner.optimizations.ctereuse.StructuralEquivalenceUtils.blocksStructurallyEquivalent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNullableValue
{
    @Test
    public void testNonComparableTypeConstants()
    {
        // This is to test the behavior of certain Constant operations.
        // Constant operation uses a NullableValue in its attribute to represent the constant value.
        // NullableValue has custom equals() and hashCode() methods that throw exception when:
        // - the type is not comparable (e.g., HyperLogLog)
        // - the value is not null (e.g., EMPTY_SLICE for HyperLogLog)
        // For that reason, Operations generally should not be compared using equals() or hashCode() methods.
        // It is consistent with the new IR design where operation semantics depends on the operation itself
        // as well as on the scope where this operation is used.

        // building two constant operations with the same type and value, and wrapping them in the structurally equivalent blocks
        Block.Parameter parameter = new Block.Parameter("%parameter", irType(anonymousRow(BIGINT, BOOLEAN)));
        Constant firstConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);
        Return firstReturn = new Return("%return", firstConstant.result(), firstConstant.attributes());
        Constant secondConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);
        Return secondReturn = new Return("%return", secondConstant.result(), secondConstant.attributes());
        Block firstBlock = new Block(
                Optional.of("^first_block"),
                ImmutableList.of(parameter),
                ImmutableList.of(firstConstant, firstReturn));
        Block secondBlock = new Block(
                Optional.of("^second_block"),
                ImmutableList.of(parameter),
                ImmutableList.of(secondConstant, secondReturn));

        // comparing identical NullableValues returns true
        NullableValue nullableValue = new NullableValue(HYPER_LOG_LOG, EMPTY_SLICE);
        assertThat(nullableValue.equals(nullableValue)).isTrue();

        // comparing a block with itself returns true because the nested NullableValues are identical
        assertThat(blocksStructurallyEquivalent(firstBlock, firstBlock)).isTrue();

        // comparing non-identical NullableValues throws an exception
        assertThatThrownBy(() -> new NullableValue(HYPER_LOG_LOG, EMPTY_SLICE).equals(new NullableValue(HYPER_LOG_LOG, EMPTY_SLICE)))
                .hasMessageMatching("Cannot invoke \"java.lang.invoke.MethodHandle.*\" because .* is null");

        // when comparing two blocks with different instances of the same NullableValue, the nested
        // comparison throws exception but the error is suppressed and the comparison returns false
        assertThat(blocksStructurallyEquivalent(firstBlock, secondBlock)).isFalse();

        // When calling the hashCode() method, it throws an exception.
        Map<Operation, Integer> map = new HashMap<>();
        assertThatThrownBy(() -> map.put(firstConstant, 0))
                .hasMessageMatching("Cannot invoke \"java.lang.invoke.MethodHandle.*\" because .* is null");
    }
}
