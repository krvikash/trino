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
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.operator.gpu.GpuTestUtils.FUNCTION_RESOLUTION;
import static io.trino.operator.gpu.SignatureFormatter.formatAggregation;
import static io.trino.operator.gpu.SignatureFormatter.formatExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSignatureFormatter
{
    @Test
    void testReference()
    {
        assertThat(formatExpression(new Reference(BIGINT, "column_name")))
                .isEqualTo("bigint");
    }

    @Test
    void testConstant()
    {
        assertThat(formatExpression(new Constant(INTEGER, 42L)))
                .isEqualTo("integer");
        assertThat(formatExpression(new Constant(BIGINT, null)))
                .isEqualTo("bigint");
    }

    @Test
    void testCast()
    {
        assertThat(formatExpression(new Cast(new Reference(INTEGER, "x"), BIGINT)))
                .isEqualTo("CAST(integer AS bigint)");
    }

    @Test
    void testOperator()
    {
        Call add = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT)),
                ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BIGINT, "b")));
        assertThat(formatExpression(add))
                .isEqualTo("$operator$add(bigint, bigint)");
    }

    @Test
    void testComparison()
    {
        Call equal = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.EQUAL, ImmutableList.of(INTEGER, INTEGER)),
                ImmutableList.of(new Reference(INTEGER, "a"), new Reference(INTEGER, "b")));
        assertThat(formatExpression(equal))
                .isEqualTo("(integer = integer)");
    }

    @Test
    void testDecimalOperator()
    {
        Call add = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(createDecimalType(38, 0), createDecimalType(38, 0))),
                ImmutableList.of(
                        new Reference(createDecimalType(38, 0), "a"),
                        new Reference(createDecimalType(38, 0), "b")));
        assertThat(formatExpression(add))
                .isEqualTo("$operator$add(decimal(38,0), decimal(38,0))");
    }

    @Test
    void testIsNull()
    {
        assertThat(formatExpression(new IsNull(new Reference(VARCHAR, "col"))))
                .isEqualTo("(varchar IS NULL)");
    }

    @Test
    void testCoalesce()
    {
        assertThat(formatExpression(new Coalesce(
                new Reference(BIGINT, "a"),
                new Reference(BIGINT, "b"))))
                .isEqualTo("COALESCE(bigint, bigint)");
    }

    @Test
    void testNestedExpression()
    {
        Call innerAdd = new Call(
                FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT)),
                ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)));
        Cast outerCast = new Cast(innerAdd, DOUBLE);
        assertThat(formatExpression(outerCast))
                .isEqualTo("CAST($operator$add(bigint, bigint) AS double)");
    }

    @Test
    void testAggregation()
    {
        Aggregation sum = new Aggregation(
                FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT)),
                ImmutableList.of(new Reference(BIGINT, "amount")),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertThat(formatAggregation(sum))
                .isEqualTo("sum(bigint)");
    }

    @Test
    void testCountStarAggregation()
    {
        Aggregation count = new Aggregation(
                FUNCTION_RESOLUTION.resolveFunction("count", fromTypes()),
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertThat(formatAggregation(count))
                .isEqualTo("count(*)");
    }

    @Test
    void testDistinctAggregation()
    {
        Aggregation countDistinct = new Aggregation(
                FUNCTION_RESOLUTION.resolveFunction("count", fromTypes(BIGINT)),
                ImmutableList.of(new Reference(BIGINT, "user_id")),
                true,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        assertThat(formatAggregation(countDistinct))
                .isEqualTo("count(DISTINCT bigint)");
    }
}
