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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.starburst.materialization.ir.Aggregation.AggregateFunctionCall;
import io.starburst.materialization.ir.Aggregation.GroupingSets;
import io.starburst.materialization.ir.Aggregation.OrderingScheme;
import io.starburst.materialization.ir.Aggregation.SortItem;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.materialization.ir.IrFixtures.builtinSignature;
import static io.starburst.materialization.ir.IrFixtures.constant;
import static io.starburst.materialization.ir.IrFixtures.outputOverScan;
import static io.starburst.materialization.ir.IrFixtures.reference;
import static io.starburst.materialization.ir.IrFixtures.tableScan;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the persisted JSON of every IR node against a checked-in fixture. A symmetric round trip cannot catch a
 * wire-format change, because both directions change together; a fixture can. A failing test here means a stored
 * definition would be read differently: bump the affected version (see {@link IrVersions}) and update the fixture.
 */
class TestIrWireFormat
{
    static Stream<Arguments> expressions()
    {
        return Stream.of(
                Arguments.of(Call.NAME, new Call(builtinSignature("$operator$EQUAL", BOOLEAN, BIGINT, BIGINT), ImmutableList.of(reference(BIGINT, "a"), reference(BIGINT, "b")))),
                Arguments.of(Cast.NAME, new Cast(reference(VARCHAR, "a"), createVarcharType(10))),
                Arguments.of(Constant.NAME, constant(BIGINT, 42L)),
                Arguments.of(FieldReference.NAME, new FieldReference(reference(IrJson.PERSON_ROW, "info"), 0)),
                Arguments.of(In.NAME, new In(reference(BIGINT, "a"), ImmutableList.of(reference(BIGINT, "b"), reference(BIGINT, "c")))),
                Arguments.of(IsNull.NAME, new IsNull(reference(BIGINT, "a"))),
                Arguments.of(Logical.NAME, new Logical(Logical.Operator.AND, ImmutableList.of(reference(BOOLEAN, "a"), reference(BOOLEAN, "b")))),
                Arguments.of(Reference.NAME, reference(BIGINT, "a")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("expressions")
    void testExpressionWireFormat(String fixture, Expression expression)
    {
        assertMatchesFixture(fixture, IrJson.codec(Expression.class), expression);
    }

    static Stream<Arguments> operations()
    {
        Output output = outputOverScan(new Symbol(BIGINT, "a"));
        return Stream.of(
                Arguments.of(Aggregation.NAME, aggregateWithEveryField()),
                Arguments.of(Output.NAME, output),
                Arguments.of(TableScan.NAME, output.source()));
    }

    private static Aggregation aggregateWithEveryField()
    {
        Symbol a = new Symbol(BIGINT, "a");
        Symbol b = new Symbol(BOOLEAN, "b");
        Symbol groupId = new Symbol(BIGINT, "g");
        AggregateFunctionCall arrayAgg = new AggregateFunctionCall(
                new Symbol(new ArrayType(BIGINT), "s"),
                builtinSignature("array_agg", new ArrayType(BIGINT), BIGINT),
                ImmutableList.of(new Reference(a)),
                true,
                Optional.of(b),
                Optional.of(new OrderingScheme(ImmutableList.of(new SortItem(a, DESC_NULLS_LAST)))),
                Optional.of(b));
        return new Aggregation(
                tableScan(a, b, groupId),
                new GroupingSets(ImmutableList.of(a, groupId), 2, ImmutableSet.of(1)),
                ImmutableList.of(arrayAgg),
                Aggregation.Step.SINGLE,
                Optional.of(groupId));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("operations")
    void testOperationWireFormat(String fixture, Operation operation)
    {
        assertMatchesFixture(fixture, IrJson.codec(Operation.class), operation);
    }

    static Stream<Arguments> constantValues()
    {
        return Stream.of(
                Arguments.of(new ConstantValue.NullValue()),
                Arguments.of(new ConstantValue.BooleanValue(true)),
                Arguments.of(new ConstantValue.LongValue(-42)),
                Arguments.of(new ConstantValue.RealValue(Float.floatToIntBits(-1.5f))),
                Arguments.of(new ConstantValue.DoubleValue(Double.doubleToLongBits(-1.5))),
                Arguments.of(new ConstantValue.BytesValue(utf8Slice("abc"))),
                Arguments.of(new ConstantValue.Int128Value(-1, -12345)),
                Arguments.of(new ConstantValue.TimestampValue(-1_000_001, 123_000)),
                Arguments.of(new ConstantValue.TimestampWithZoneValue(-1_000, 123_000_000, "America/New_York")),
                Arguments.of(new ConstantValue.TimeWithZoneValue(12_345_000_000_000L, -480)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("constantValues")
    void testConstantValueWireFormat(ConstantValue value)
    {
        assertMatchesFixture(constantValueFixture(value.kind()), IrJson.codec(ConstantValue.class), value);
    }

    static String constantValueFixture(String kind)
    {
        return "ConstantValue_" + kind;
    }

    @Test
    void testBoundSignatureWireFormat()
    {
        assertMatchesFixture("BoundSignature", IrJson.codec(BoundSignature.class), builtinSignature("$operator$LESS_THAN", BOOLEAN, VARCHAR, VARCHAR));
    }

    static <T> void assertMatchesFixture(String fixture, JsonCodec<T> codec, T value)
    {
        String expected = fixture(fixture);
        assertThat(codec.toJson(value)).as("fixture %s", fixture).isEqualTo(expected);
        assertThat(codec.fromJson(expected)).as("fixture %s", fixture).isEqualTo(value);
    }

    static boolean fixtureExists(String fixture)
    {
        return TestIrWireFormat.class.getResource(fixturePath(fixture)) != null;
    }

    private static String fixture(String fixture)
    {
        try {
            return Resources.toString(Resources.getResource(TestIrWireFormat.class, fixturePath(fixture)), UTF_8).strip();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String fixturePath(String fixture)
    {
        return "golden/%s.json".formatted(fixture);
    }
}
