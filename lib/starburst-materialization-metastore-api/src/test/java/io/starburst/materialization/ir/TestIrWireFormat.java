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
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

import static io.starburst.materialization.ir.IrFixtures.outputOverScan;
import static io.starburst.materialization.ir.TestExpressionValidation.reference;
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
                Arguments.of(Cast.NAME, new Cast(reference(VARCHAR, "a"), createVarcharType(10))),
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
                Arguments.of(Output.NAME, output),
                Arguments.of(TableScan.NAME, output.source()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("operations")
    void testOperationWireFormat(String fixture, Operation operation)
    {
        assertMatchesFixture(fixture, IrJson.codec(Operation.class), operation);
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
