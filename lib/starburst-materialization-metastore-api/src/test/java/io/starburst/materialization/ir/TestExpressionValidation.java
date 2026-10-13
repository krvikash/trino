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
import io.starburst.materialization.ir.ConstantValue.BytesValue;
import io.starburst.materialization.ir.ConstantValue.TimestampWithZoneValue;
import io.starburst.materialization.ir.Logical.Operator;
import io.starburst.materialization.ir.SupportedCasts.CastPair;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.materialization.ir.IrFixtures.builtinSignature;
import static io.starburst.materialization.ir.IrFixtures.constant;
import static io.starburst.materialization.ir.IrFixtures.reference;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.StandardTypes.CHAR;
import static io.trino.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestExpressionValidation
{
    private static final Set<String> TIME_ZONE_BEARING = ImmutableSet.of(TIMESTAMP_WITH_TIME_ZONE, TIME_WITH_TIME_ZONE);

    @Test
    void testCastBetweenVarcharsIsAccepted()
    {
        Cast cast = new Cast(reference(createUnboundedVarcharType(), "a"), createVarcharType(10));
        assertThat(cast.type()).isEqualTo(createVarcharType(10));
    }

    @Test
    void testUnfoldedJsonPathCastIsAccepted()
    {
        Cast cast = new Cast(constant(createUnboundedVarcharType(), utf8Slice("$.name")), IrFixtures.JSON_PATH);
        assertThat(cast.type()).isEqualTo(IrFixtures.JSON_PATH);
    }

    @Test
    void testCastOffTheAllowlistIsRejected()
    {
        assertThatThrownBy(() -> new Cast(reference(BIGINT, "secret_column"), createVarcharType(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cast: cast from bigint to varchar is not supported");
        assertThat(SupportedCasts.contains("bigint", VARCHAR)).isFalse();
    }

    @Test
    void testCastAcrossTimeZoneBoundaryIsRejected()
    {
        assertThatThrownBy(() -> new Cast(reference(TIMESTAMP_MILLIS, "a"), TIMESTAMP_TZ_MILLIS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cast: cast from timestamp to timestamp with time zone is not supported");
    }

    @Test
    void testCastFromCharToVarcharIsRejected()
    {
        assertThatThrownBy(() -> new Cast(reference(createCharType(5), "a"), createVarcharType(10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cast: cast from char to varchar is not supported");
    }

    @Test
    void testCastRegistryExcludesSessionDependentFamilies()
    {
        assertThat(SupportedCasts.supported()).isNotEmpty();
        for (CastPair pair : SupportedCasts.supported()) {
            assertThat(TIME_ZONE_BEARING.contains(pair.sourceBaseName()))
                    .as("time zone crossing cast %s", pair)
                    .isEqualTo(TIME_ZONE_BEARING.contains(pair.targetBaseName()));
            assertThat(ImmutableSet.of(pair.sourceBaseName(), pair.targetBaseName()))
                    .as("char/varchar cast %s", pair)
                    .isNotEqualTo(ImmutableSet.of(CHAR, VARCHAR));
        }
    }

    @Test
    void testCallOffTheAllowlistIsRejected()
    {
        BoundSignature random = builtinSignature("random", DOUBLE);
        assertThat(SupportedFunctions.contains(random.getName())).isFalse();
        assertThatThrownBy(() -> new Call(random, ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Call: function system.builtin.random is not supported");
    }

    @Test
    void testCallWithArityMismatchIsRejected()
    {
        assertThatThrownBy(() -> new Call(builtinSignature("$operator$EQUAL", BOOLEAN, BIGINT, BIGINT), ImmutableList.of(reference(BIGINT, "secret_column"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Call: function system.builtin.$operator$equal expects 2 arguments, got 1");
    }

    @Test
    void testCallWithArgumentTypeMismatchIsRejected()
    {
        assertThatThrownBy(() -> new Call(
                builtinSignature("$operator$EQUAL", BOOLEAN, BIGINT, BIGINT),
                ImmutableList.of(reference(BIGINT, "a"), reference(IrJson.PERSON_ROW, "secret_column"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Call: argument 1 of function system.builtin.$operator$equal has type row, expected bigint");
    }

    @Test
    void testComparisonOfTimestampsWithTimeZoneIsAccepted()
    {
        Call call = new Call(
                builtinSignature("$operator$LESS_THAN", BOOLEAN, TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MILLIS),
                ImmutableList.of(reference(TIMESTAMP_TZ_MILLIS, "a"), reference(TIMESTAMP_TZ_MILLIS, "b")));
        assertThat(call.type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testFieldReferenceIntoRowIsAccepted()
    {
        FieldReference name = new FieldReference(reference(IrJson.PERSON_ROW, "info"), 0);
        assertThat(name.type()).isEqualTo(createUnboundedVarcharType());
    }

    @Test
    void testFieldReferenceIntoNonRowIsRejected()
    {
        assertThatThrownBy(() -> new FieldReference(reference(BIGINT, "secret_column"), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FieldReference: base has type bigint, expected a row type");
    }

    @Test
    void testFieldReferenceOutOfBoundsIsRejected()
    {
        assertThatThrownBy(() -> new FieldReference(reference(IrJson.PERSON_ROW, "secret_column"), 2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FieldReference: field 2 is out of bounds for a row with 2 fields");
        assertThatThrownBy(() -> new FieldReference(reference(IrJson.PERSON_ROW, "secret_column"), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FieldReference: field -1 is out of bounds for a row with 2 fields");
    }

    @Test
    void testLogicalWithOneTermIsRejected()
    {
        assertThatThrownBy(() -> new Logical(Operator.AND, ImmutableList.of(reference(BOOLEAN, "a"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Logical: expected at least 2 terms, got 1");
    }

    @Test
    void testLogicalWithNonBooleanTermIsRejected()
    {
        assertThatThrownBy(() -> new Logical(Operator.OR, ImmutableList.of(reference(BOOLEAN, "a"), reference(BIGINT, "b"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Logical: term 1 has type bigint, expected boolean");
    }

    @Test
    void testInOverReferencesIsAccepted()
    {
        In in = new In(reference(BIGINT, "a"), ImmutableList.of(reference(BIGINT, "b"), reference(BIGINT, "c")));
        assertThat(in.type()).isEqualTo(BOOLEAN);
        assertThat(in.children()).hasSize(3);
    }

    @Test
    void testInWithEmptyValueListIsRejected()
    {
        assertThatThrownBy(() -> new In(reference(BIGINT, "a"), ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("In: value list is empty");
    }

    @Test
    void testInWithTypeMismatchedItemIsRejected()
    {
        assertThatThrownBy(() -> new In(reference(BIGINT, "a"), ImmutableList.of(reference(BIGINT, "b"), reference(BOOLEAN, "c"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("In: item 1 has type boolean, expected bigint");
    }

    @Test
    void testConstantWithMismatchedKindIsRejected()
    {
        assertThatThrownBy(() -> new Constant(DOUBLE, new ConstantValue.LongValue(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Constant: cannot decode long constant of type double: expected kind double");
    }

    @Test
    void testConstantOfUnsupportedTypeIsRejected()
    {
        assertThatThrownBy(() -> new Constant(new ArrayType(BIGINT), new ConstantValue.NullValue()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Constant: cannot decode null constant of type array(bigint): type is not supported");
    }

    @Test
    void testInOverConstantsIsAccepted()
    {
        In in = new In(reference(BIGINT, "a"), ImmutableList.of(constant(BIGINT, 1L), constant(BIGINT, 2L)));
        assertThat(in.type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testComparisonWithConstantIsAccepted()
    {
        Call call = new Call(builtinSignature("$operator$LESS_THAN", BOOLEAN, BIGINT, BIGINT), ImmutableList.of(reference(BIGINT, "a"), constant(BIGINT, 42L)));
        assertThat(call.type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testComparisonWithFoldedTimestampWithTimeZoneConstantIsAccepted()
    {
        Call call = new Call(
                builtinSignature("$operator$LESS_THAN", BOOLEAN, TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MILLIS),
                ImmutableList.of(reference(TIMESTAMP_TZ_MILLIS, "a"), new Constant(TIMESTAMP_TZ_MILLIS, new TimestampWithZoneValue(1_577_836_800_000L, 0, "America/New_York"))));
        assertThat(call.type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testComparisonWithFoldedCharConstantIsAccepted()
    {
        Call call = new Call(
                builtinSignature("$operator$EQUAL", BOOLEAN, createCharType(5), createCharType(5)),
                ImmutableList.of(reference(createCharType(5), "a"), new Constant(createCharType(5), new BytesValue(utf8Slice("abc")))));
        assertThat(call.type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testIsNullIsBoolean()
    {
        assertThat(new IsNull(reference(BIGINT, "a")).type()).isEqualTo(BOOLEAN);
    }
}
