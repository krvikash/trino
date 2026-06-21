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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * @see io.trino.operator.gpu.TestGpuExpressions for tests covering expression execution
 */
class TestGpuExpressionCompiler
{
    @Test
    void testEveryExpressionConsidered()
            throws Exception
    {
        assertAllMethodsOverridden(IrVisitor.class, GpuExpressionCompiler.CompilationVisitor.class, Set.of(
                // has good default
                IrVisitor.class.getMethod("process", Expression.class, Object.class /*context*/)));
    }

    @ParameterizedTest
    @MethodSource("operandTypes")
    void testComparisonAcceptsType(Type type, boolean expectGpuCompile)
    {
        Reference left = new Reference(type, "a");
        Reference right = new Reference(type, "b");
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(type, "a"), 0,
                new Symbol(type, "b"), 1);
        assertThat(compileExpression(comparison(ComparisonOperator.EQUAL, left, right), layout).isPresent())
                .as("Comparison on %s", type)
                .isEqualTo(expectGpuCompile);
    }

    @ParameterizedTest
    @MethodSource("operandTypes")
    void testBetweenAcceptsType(Type type, boolean expectGpuCompile)
    {
        if (!type.isOrderable()) {
            // BETWEEN requires an orderable operand; IrExpressions.between cannot resolve $operator$less_than_or_equal
            abort("BETWEEN requires an orderable type: " + type);
        }
        Reference value = new Reference(type, "a");
        Reference min = new Reference(type, "b");
        Reference max = new Reference(type, "c");
        Map<Symbol, Integer> layout = ImmutableMap.of(
                new Symbol(type, "a"), 0,
                new Symbol(type, "b"), 1,
                new Symbol(type, "c"), 2);
        assertThat(compileExpression(between(value, min, max), layout).isPresent())
                .as("Between on %s", type)
                .isEqualTo(expectGpuCompile);
    }

    @ParameterizedTest
    @MethodSource("operandTypes")
    void testInAcceptsType(Type type, boolean expectGpuCompile)
    {
        Reference value = new Reference(type, "a");
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(type, "a"), 0);
        List<Expression> valueList = ImmutableList.of(new Constant(type, null));
        assertThat(compileExpression(new In(value, valueList), layout).isPresent())
                .as("In on %s", type)
                .isEqualTo(expectGpuCompile);
    }

    static Stream<Arguments> operandTypes()
    {
        return Stream.of(
                arguments(BOOLEAN, true),
                arguments(TINYINT, true),
                arguments(SMALLINT, true),
                arguments(INTEGER, true),
                arguments(BIGINT, true),
                arguments(REAL, true),
                arguments(DOUBLE, true),
                arguments(createDecimalType(5, 0), true),
                arguments(createDecimalType(5, 2), true),
                arguments(createDecimalType(10, 0), true),
                arguments(createDecimalType(10, 2), true),
                arguments(createDecimalType(20, 0), true),
                arguments(createDecimalType(20, 2), true),
                arguments(createDecimalType(38, 0), true),
                arguments(createDecimalType(38, 2), true),
                arguments(NUMBER, false),
                arguments(createCharType(8), true),
                arguments(VARCHAR, true),
                arguments(createVarcharType(20), true),
                arguments(VARBINARY, false),
                arguments(DATE, true),
                arguments(createTimeType(0), false),
                arguments(createTimeType(1), false),
                arguments(createTimeType(2), false),
                arguments(createTimeType(3), false),
                arguments(createTimeType(4), false),
                arguments(createTimeType(5), false),
                arguments(createTimeType(6), false),
                arguments(createTimeType(7), false),
                arguments(createTimeType(8), false),
                arguments(createTimeType(9), false),
                arguments(createTimeType(10), false),
                arguments(createTimeType(11), false),
                arguments(createTimeType(12), false),
                arguments(createTimeWithTimeZoneType(0), false),
                arguments(createTimeWithTimeZoneType(1), false),
                arguments(createTimeWithTimeZoneType(2), false),
                arguments(createTimeWithTimeZoneType(3), false),
                arguments(createTimeWithTimeZoneType(4), false),
                arguments(createTimeWithTimeZoneType(5), false),
                arguments(createTimeWithTimeZoneType(6), false),
                arguments(createTimeWithTimeZoneType(7), false),
                arguments(createTimeWithTimeZoneType(8), false),
                arguments(createTimeWithTimeZoneType(9), false),
                arguments(createTimeWithTimeZoneType(10), false),
                arguments(createTimeWithTimeZoneType(11), false),
                arguments(createTimeWithTimeZoneType(12), false),
                arguments(createTimestampType(0), true),
                arguments(createTimestampType(1), true),
                arguments(createTimestampType(2), true),
                arguments(createTimestampType(3), true),
                arguments(createTimestampType(4), true),
                arguments(createTimestampType(5), true),
                arguments(createTimestampType(6), true),
                arguments(createTimestampType(7), true),
                arguments(createTimestampType(8), true),
                arguments(createTimestampType(9), true),
                arguments(createTimestampType(10), false),
                arguments(createTimestampType(11), false),
                arguments(createTimestampType(12), false),
                arguments(createTimestampWithTimeZoneType(0), false),
                arguments(createTimestampWithTimeZoneType(1), false),
                arguments(createTimestampWithTimeZoneType(2), false),
                arguments(createTimestampWithTimeZoneType(3), false),
                arguments(createTimestampWithTimeZoneType(4), false),
                arguments(createTimestampWithTimeZoneType(5), false),
                arguments(createTimestampWithTimeZoneType(6), false),
                arguments(createTimestampWithTimeZoneType(7), false),
                arguments(createTimestampWithTimeZoneType(8), false),
                arguments(createTimestampWithTimeZoneType(9), false),
                arguments(createTimestampWithTimeZoneType(10), false),
                arguments(createTimestampWithTimeZoneType(11), false),
                arguments(createTimestampWithTimeZoneType(12), false),
                arguments(new ArrayType(BIGINT), false),
                arguments(new MapType(BIGINT, BIGINT, new TypeOperators()), false),
                arguments(RowType.from(ImmutableList.of(RowType.field("a", BIGINT))), false));
    }
}
