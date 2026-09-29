/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.starburst.materialization.ir.Call;
import io.starburst.materialization.ir.Cast;
import io.starburst.materialization.ir.Expression;
import io.starburst.materialization.ir.FieldReference;
import io.starburst.materialization.ir.In;
import io.starburst.materialization.ir.IsNull;
import io.starburst.materialization.ir.Logical;
import io.starburst.materialization.ir.Reference;
import io.starburst.materialization.ir.Symbol;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.substitution.TestUtils.irJsonCodecFactory;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trips each Expression IR node type through JSON, resolving types through the engine's type manager, and
 * checks that each deserialization-time validation fails as the metastore expects. The codec comes from
 * {@link TestUtils#irJsonCodecFactory()}, the same factory the test metastore uses for its {@code Output} codec.
 */
// TODO(ENG-12666): no persisted document has a slot for an Expression yet. Once Filter adds one, run these cases
//  through the Output codec, with each expression as a Filter predicate in an Output tree, instead of a standalone
//  Expression codec.
class TestMaterializationExpressionJson
{
    private static final JsonCodec<Expression> CODEC = irJsonCodecFactory().jsonCodec(Expression.class);
    private static final String SECRET = "secret_";
    private static final RowType INFO = RowType.from(ImmutableList.of(
            RowType.field(SECRET + "name", VARCHAR),
            RowType.field(SECRET + "age", BIGINT)));

    @ParameterizedTest(name = "{0}")
    @MethodSource("expressions")
    void testRoundTrip(String scenario, Expression expression)
    {
        assertThat(CODEC.fromJson(CODEC.toJson(expression))).isEqualTo(expression);
    }

    private static Stream<Arguments> expressions()
    {
        Type nestedType = RowType.from(ImmutableList.of(
                RowType.field("items", new ArrayType(RowType.from(ImmutableList.of(RowType.field("x", BIGINT))))),
                RowType.field("tags", new MapType(VARCHAR, BIGINT, new TypeOperators()))));
        return Stream.of(
                Arguments.of("comparison", equal(reference(BIGINT, "a"), reference(BIGINT, "b"))),
                Arguments.of("negated comparison", not(call("$operator$IDENTICAL", BOOLEAN, reference(BIGINT, "a"), reference(BIGINT, "b")))),
                Arguments.of("time zone comparison", call("$operator$LESS_THAN", BOOLEAN, reference(TIMESTAMP_TZ_MILLIS, "a"), reference(TIMESTAMP_TZ_MILLIS, "b"))),
                Arguments.of("cast", new Cast(reference(VARCHAR, "a"), createVarcharType(10))),
                Arguments.of("row field access", new FieldReference(reference(INFO, "info"), 0)),
                Arguments.of("logical", new Logical(Logical.Operator.OR, ImmutableList.of(reference(BOOLEAN, "a"), new IsNull(reference(BIGINT, "b"))))),
                Arguments.of("in", new In(reference(BIGINT, "a"), ImmutableList.of(reference(BIGINT, "b"), reference(BIGINT, "c")))),
                Arguments.of("nested row and array types", new IsNull(reference(nestedType, "a"))),
                Arguments.of("json type", new IsNull(reference(JSON, "a"))),
                Arguments.of("ipaddress type", new IsNull(reference(IPADDRESS, "a"))));
    }

    /**
     * Every failure surfaces as a {@link RuntimeException}, the type the metastore catches to skip an unreadable
     * row, and its root cause message carries no symbol or row field name.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("deserializationFailures")
    void testUnreadableExpression(String scenario, Expression valid, UnaryOperator<String> corruptJson, Optional<String> expectedRootCauseMessage)
    {
        String json = corruptJson.apply(CODEC.toJson(valid));
        assertThatThrownBy(() -> CODEC.fromJson(json))
                .isInstanceOf(RuntimeException.class)
                .satisfies(failure -> {
                    String rootCauseMessage = Throwables.getRootCause(failure).getMessage();
                    assertThat(rootCauseMessage).doesNotContain(SECRET);
                    expectedRootCauseMessage.ifPresent(expected -> assertThat(rootCauseMessage).isEqualTo(expected));
                });
    }

    private static Stream<Arguments> deserializationFailures()
    {
        Expression comparison = equal(new FieldReference(reference(INFO, "info"), 0), reference(VARCHAR, "b"));
        IsNull secondTerm = new IsNull(reference(BIGINT, "b"));
        Expression logical = new Logical(Logical.Operator.AND, ImmutableList.of(new IsNull(reference(BIGINT, "a")), secondTerm));
        Expression cast = new Cast(reference(VARCHAR, "a"), createVarcharType(10));
        return Stream.of(
                Arguments.of(
                        "function off the allowlist",
                        comparison,
                        replacingToken("$operator$equal", "random"),
                        Optional.of("Call: function system.builtin.random is not supported")),
                Arguments.of(
                        "argument count disagrees with signature",
                        comparison,
                        replacingToken("\"argumentTypes\":[\"varchar\",\"varchar\"]", "\"argumentTypes\":[\"varchar\",\"varchar\",\"varchar\"]"),
                        Optional.of("Call: function system.builtin.$operator$equal expects 3 arguments, got 2")),
                Arguments.of(
                        "argument type disagrees with signature",
                        comparison,
                        replacingToken("\"argumentTypes\":[\"varchar\",\"varchar\"]", "\"argumentTypes\":[\"varchar\",\"bigint\"]"),
                        Optional.of("Call: argument 1 of function system.builtin.$operator$equal has type varchar, expected bigint")),
                Arguments.of(
                        "cast pair off the allowlist",
                        cast,
                        replacingToken("\"type\":\"varchar(10)\"", "\"type\":\"bigint\""),
                        Optional.of("Cast: cast from varchar to bigint is not supported")),
                Arguments.of(
                        "logical with one term",
                        logical,
                        replacingToken("," + CODEC.toJson(secondTerm), ""),
                        Optional.of("Logical: expected at least 2 terms, got 1")),
                Arguments.of(
                        "field index out of bounds",
                        comparison,
                        replacingToken("\"field\":0", "\"field\":5"),
                        Optional.of("FieldReference: field 5 is out of bounds for a row with 2 fields")),
                Arguments.of(
                        "unknown node type",
                        logical,
                        replacingToken("\"@type\":\"IsNull\"", "\"@type\":\"NoSuchNode\""),
                        Optional.empty()),
                Arguments.of(
                        "unresolvable reference type",
                        logical,
                        replacingToken("\"type\":\"bigint\"", "\"type\":\"no_such_type\""),
                        Optional.empty()));
    }

    /**
     * Replaces every {@code token} in the JSON with {@code replacement}, asserting {@code token} is present first so a
     * fixture change can't silently turn this into a no-op.
     */
    private static UnaryOperator<String> replacingToken(String token, String replacement)
    {
        return json -> {
            assertThat(json).contains(token);
            return json.replace(token, replacement);
        };
    }

    private static Reference reference(Type type, String name)
    {
        return new Reference(new Symbol(type, SECRET + name));
    }

    private static Call equal(Expression left, Expression right)
    {
        return call("$operator$EQUAL", BOOLEAN, left, right);
    }

    private static Call not(Expression value)
    {
        return call("$not", BOOLEAN, value);
    }

    private static Call call(String functionName, Type returnType, Expression... arguments)
    {
        ImmutableList<Expression> argumentList = ImmutableList.copyOf(arguments);
        BoundSignature signature = new BoundSignature(
                new CatalogSchemaFunctionName("system", "builtin", functionName),
                returnType,
                argumentList.stream().map(Expression::type).collect(toImmutableList()));
        return new Call(signature, argumentList);
    }
}
