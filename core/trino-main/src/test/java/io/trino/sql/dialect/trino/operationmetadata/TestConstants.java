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
package io.trino.sql.dialect.trino.operationmetadata;

import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createTimestampsWithTimeZoneMillisBlock;
import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_NOT_DISTINCT_FROM;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.DEFAULT_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue.isOrContainsTypeComparedByValue;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.invoke.MethodType.methodType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConstants
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    /**
     * This is to test the behavior of Constant operations involving values of non-comparable types.
     * Constant operation uses a ConstantValue object in its attribute to represent the constant value.
     * ConstantValue is similar to NullableValue in that it supports serialization and semantic comparison.
     * Additionally, ConstantValue objects implement safe equals() and hashCode() methods that do not
     * throw exceptions when the underlying type is non-comparable, while NullableValue's equals() and hashCode()
     * methods may throw exceptions in such cases.
     */
    @Test
    public void testNonComparableTypeConstants()
    {
        // comparing non-identical ConstantValues returns false
        assertThat(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).equals(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE)))
                .isFalse();

        // calling hashCode() on ConstantValue of incomparable type succeeds
        assertThat(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).hashCode())
                .isEqualTo(ConstantValue.of(HYPER_LOG_LOG, EMPTY_SLICE).hashCode());

        Constant firstConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);
        Constant secondConstant = new Constant("%constant", HYPER_LOG_LOG, EMPTY_SLICE);

        // comparing constant operations does not throw exception
        assertThat(firstConstant.equals(secondConstant)).isFalse();

        // calling hashCode() on constant operation does not throw exception
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
    }

    @Test
    public void testNullConstants()
    {
        Constant firstConstant = new Constant("%constant", HYPER_LOG_LOG, null);
        Constant secondConstant = new Constant("%constant", HYPER_LOG_LOG, null);

        // two Constant operations representing null values are equal
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);
    }

    @Test
    public void testShortTimestampWithTimeZone()
            throws Throwable
    {
        // two different representations of the same instant in time. They use different time zones.
        Object firstValue = parseTimestampWithTimeZone(3, "1970-01-01 00:01:00.000 UTC");
        Object secondValue = parseTimestampWithTimeZone(3, "1970-01-01 08:01:00.000 +08:00");

        // the two values of type TIMESTAMP_TZ_MILLIS are considered equal according to the type's equal operator
        MethodHandle equalOperator = TYPE_OPERATORS.getEqualOperator(TIMESTAMP_TZ_MILLIS, simpleConvention(DEFAULT_ON_NULL, NEVER_NULL, NEVER_NULL))
                .asType(methodType(boolean.class, Object.class, Object.class));
        assertThat((boolean) equalOperator.invokeExact(firstValue, secondValue)).isTrue();

        // however, their Java representations are not equal
        assertThat(firstValue).isNotEqualTo(secondValue);

        // create Constant operations wrapping the two values
        Constant firstConstant = new Constant("%constant", TIMESTAMP_TZ_MILLIS, firstValue);
        Constant secondConstant = new Constant("%constant", TIMESTAMP_TZ_MILLIS, secondValue);

        // the Constant operations are not equal according to their equals() method
        // this way we avoid merging and reusing Constant operations that have different representations
        // reusing them would be correct in the context of predicate, but not in the context of output where time zone matters
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdValue = parseTimestampWithTimeZone(3, "1970-01-01 00:01:00.000 UTC");
        assertThat(thirdValue).isEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", TIMESTAMP_TZ_MILLIS, thirdValue);
        // the Constant operations are equal according to their equals() method
        assertThat(thirdConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(firstConstant);
    }

    @Test
    public void testLongTimestampWithTimeZone()
            throws Throwable
    {
        // two different representations of the same instant in time. They use different time zones.
        Object firstValue = parseTimestampWithTimeZone(12, "1970-01-01 00:01:00.000000000000 UTC");
        Object secondValue = parseTimestampWithTimeZone(12, "1970-01-01 08:01:00.000000000000 +08:00");

        // the two values of type TIMESTAMP_TZ_PICOS are considered equal according to the type's equal operator
        MethodHandle equalOperator = TYPE_OPERATORS.getEqualOperator(TIMESTAMP_TZ_PICOS, simpleConvention(DEFAULT_ON_NULL, NEVER_NULL, NEVER_NULL))
                .asType(methodType(boolean.class, Object.class, Object.class));
        assertThat((boolean) equalOperator.invokeExact(firstValue, secondValue)).isTrue();

        // however, their Java representations are not equal
        assertThat(firstValue).isNotEqualTo(secondValue);

        // create Constant operations wrapping the two values
        Constant firstConstant = new Constant("%constant", TIMESTAMP_TZ_PICOS, firstValue);
        Constant secondConstant = new Constant("%constant", TIMESTAMP_TZ_PICOS, secondValue);

        // the Constant operations are not equal according to their equals() method
        // this way we avoid merging and reusing Constant operations that have different representations
        // reusing them would be correct in the context of predicate, but not in the context of output where time zone matters
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdValue = parseTimestampWithTimeZone(12, "1970-01-01 00:01:00.000000000000 UTC");
        assertThat(thirdValue).isEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", TIMESTAMP_TZ_PICOS, thirdValue);
        // the Constant operations are equal according to their equals() method
        assertThat(thirdConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(firstConstant);
    }

    @Test
    public void testShortTimeWithTimeZone()
            throws Throwable
    {
        // two different representations of the same time. They use different time zones.
        Object firstValue = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        Object secondValue = parseTimeWithTimeZone(3, "08:01:00.000 +08:00");

        // the two values of type TIME_TZ_MILLIS are considered equal according to the type's equal operator
        MethodHandle equalOperator = TYPE_OPERATORS.getEqualOperator(TIME_TZ_MILLIS, simpleConvention(DEFAULT_ON_NULL, NEVER_NULL, NEVER_NULL))
                .asType(methodType(boolean.class, Object.class, Object.class));
        assertThat((boolean) equalOperator.invokeExact(firstValue, secondValue)).isTrue();

        // however, their Java representations are not equal
        assertThat(firstValue).isNotEqualTo(secondValue);

        // create Constant operations wrapping the two values
        Constant firstConstant = new Constant("%constant", TIME_TZ_MILLIS, firstValue);
        Constant secondConstant = new Constant("%constant", TIME_TZ_MILLIS, secondValue);

        // the Constant operations are not equal according to their equals() method
        // this way we avoid merging and reusing Constant operations that have different representations
        // reusing them would be correct in the context of predicate, but not in the context of output where time zone matters
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdValue = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        assertThat(thirdValue).isEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", TIME_TZ_MILLIS, thirdValue);
        // the Constant operations are equal according to their equals() method
        assertThat(thirdConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(firstConstant);
    }

    @Test
    public void testLongTimeWithTimeZone()
            throws Throwable
    {
        // two different representations of the same time. They use different time zones.
        Object firstValue = parseTimeWithTimeZone(12, "00:01:00.000000000000 +00:00");
        Object secondValue = parseTimeWithTimeZone(12, "08:01:00.000000000000 +08:00");

        // the two values of type TIME_TZ_PICOS are considered equal according to the type's equal operator
        MethodHandle equalOperator = TYPE_OPERATORS.getEqualOperator(TIME_TZ_PICOS, simpleConvention(DEFAULT_ON_NULL, NEVER_NULL, NEVER_NULL))
                .asType(methodType(boolean.class, Object.class, Object.class));
        assertThat((boolean) equalOperator.invokeExact(firstValue, secondValue)).isTrue();

        // however, their Java representations are not equal
        assertThat(firstValue).isNotEqualTo(secondValue);

        // create Constant operations wrapping the two values
        Constant firstConstant = new Constant("%constant", TIME_TZ_PICOS, firstValue);
        Constant secondConstant = new Constant("%constant", TIME_TZ_PICOS, secondValue);

        // the Constant operations are not equal according to their equals() method
        // this way we avoid merging and reusing Constant operations that have different representations
        // reusing them would be correct in the context of predicate, but not in the context of output where time zone matters
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdValue = parseTimeWithTimeZone(12, "00:01:00.000000000000 +00:00");
        assertThat(thirdValue).isEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", TIME_TZ_PICOS, thirdValue);
        // the Constant operations are equal according to their equals() method
        assertThat(thirdConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(firstConstant);
    }

    @Test
    public void testDouble()
    {
        // Two different NaN representations
        double firstNaN = longBitsToDouble(0x7FF0000000000001L);
        double secondNaN = longBitsToDouble(0x7FFFFFFFFFFFFFFFL);
        assertThat(Double.isNaN(firstNaN)).isTrue();
        assertThat(Double.isNaN(secondNaN)).isTrue();
        // their Java representations are not equal
        assertThat(firstNaN).isNotEqualTo(secondNaN);
        // Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", DOUBLE, firstNaN);
        Constant secondConstant = new Constant("%constant", DOUBLE, secondNaN);
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);

        // positive zero and negative zero -- equal
        double positiveZero = 0.0;
        double negativeZero = -0.0;
        assertThat(Double.valueOf(positiveZero)).isNotEqualTo(Double.valueOf(negativeZero));
        Constant positiveZeroConstant = new Constant("%constant", DOUBLE, positiveZero);
        Constant negativeZeroConstant = new Constant("%constant", DOUBLE, negativeZero);
        assertThat(positiveZeroConstant.hashCode()).isEqualTo(negativeZeroConstant.hashCode());
        assertThat(positiveZeroConstant).isEqualTo(negativeZeroConstant);

        // positive infinity and negative infinity -- not equal
        double positiveInfinity = Double.POSITIVE_INFINITY;
        double negativeInfinity = Double.NEGATIVE_INFINITY;
        assertThat(positiveInfinity).isNotEqualTo(negativeInfinity);
        Constant positiveInfinityConstant = new Constant("%constant", DOUBLE, positiveInfinity);
        Constant negativeInfinityConstant = new Constant("%constant", DOUBLE, negativeInfinity);
        assertThat(positiveInfinityConstant.hashCode()).isNotEqualTo(negativeInfinityConstant.hashCode());
        assertThat(positiveInfinityConstant).isNotEqualTo(negativeInfinityConstant);
    }

    @Test
    public void testReal()
    {
        // Two different NaN representations
        float firstNaN = intBitsToFloat(0x7F800001);
        float secondNaN = intBitsToFloat(0x7FFFFFFF);
        assertThat(Float.isNaN(firstNaN)).isTrue();
        assertThat(Float.isNaN(secondNaN)).isTrue();
        // their Java representations are not equal
        assertThat(firstNaN).isNotEqualTo(secondNaN);
        // Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", REAL, (long) floatToIntBits(firstNaN));
        Constant secondConstant = new Constant("%constant", REAL, (long) floatToIntBits(secondNaN));
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);

        // positive zero and negative zero -- equal
        float positiveZero = 0.0f;
        float negativeZero = -0.0f;
        assertThat(Float.valueOf(positiveZero)).isNotEqualTo(Float.valueOf(negativeZero));
        Constant positiveZeroConstant = new Constant("%constant", REAL, (long) floatToIntBits(positiveZero));
        Constant negativeZeroConstant = new Constant("%constant", REAL, (long) floatToIntBits(negativeZero));
        assertThat(positiveZeroConstant.hashCode()).isEqualTo(negativeZeroConstant.hashCode());
        assertThat(positiveZeroConstant).isEqualTo(negativeZeroConstant);

        // positive infinity and negative infinity -- not equal
        float positiveInfinity = Float.POSITIVE_INFINITY;
        float negativeInfinity = Float.NEGATIVE_INFINITY;
        assertThat(positiveInfinity).isNotEqualTo(negativeInfinity);
        Constant positiveInfinityConstant = new Constant("%constant", REAL, (long) floatToIntBits(positiveInfinity));
        Constant negativeInfinityConstant = new Constant("%constant", REAL, (long) floatToIntBits(negativeInfinity));
        assertThat(positiveInfinityConstant.hashCode()).isNotEqualTo(negativeInfinityConstant.hashCode());
        assertThat(positiveInfinityConstant).isNotEqualTo(negativeInfinityConstant);
    }

    @Test
    public void testBooleanArray()
    {
        // array of boolean is compared by IDENTICAL
        ArrayType booleanArray = new ArrayType(BOOLEAN);
        assertThat(isOrContainsTypeComparedByValue(booleanArray)).isFalse();

        // two separate Blocks with the same content are not equal
        Block firstValue = createBooleansBlock(true, false);
        Block secondValue = createBooleansBlock(true, false);
        assertThat(firstValue).isNotEqualTo(secondValue);
        // but Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", booleanArray, firstValue);
        Constant secondConstant = new Constant("%constant", booleanArray, secondValue);
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);

        // two Blocks with the same content including nulls
        Block thirdValue = createBooleansBlock(true, null);
        Block fourthValue = createBooleansBlock(true, null);
        assertThat(thirdValue).isNotEqualTo(fourthValue);
        // but Constant operations wrapping them are equal. They are compared by IDENTICAL including nulls
        Constant thirdConstant = new Constant("%constant", booleanArray, thirdValue);
        Constant fourthConstant = new Constant("%constant", booleanArray, fourthValue);
        assertThat(thirdConstant.hashCode()).isEqualTo(fourthConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(fourthConstant);

        // two Blocks with different content are not equal
        Block fifthValue = createBooleansBlock(false);
        assertThat(firstValue).isNotEqualTo(fifthValue);
        // and Constant operations wrapping them are not equal
        Constant fifthConstant = new Constant("%constant", booleanArray, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testTimestampArray()
    {
        // array of timestamp with time zone is compared by value (equals() method of the underlying Block)
        ArrayType timestampArray = new ArrayType(TIMESTAMP_TZ_MILLIS);
        assertThat(isOrContainsTypeComparedByValue(timestampArray)).isTrue();

        // two different representations of the same time. They use different time zones.
        Object firstTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        Object secondTimestamp = parseTimeWithTimeZone(3, "08:01:00.000 +08:00");
        assertThat(firstTimestamp).isNotEqualTo(secondTimestamp);
        // blocks wrapping the two values are not equal
        Block firstValue = createTimestampsWithTimeZoneMillisBlock((long) firstTimestamp);
        Block secondValue = createTimestampsWithTimeZoneMillisBlock((long) secondTimestamp);
        assertThat(firstValue).isNotEqualTo(secondValue);
        // Constant operations wrapping them are also not equal since array of timestamp with time zone is compared by value (block)
        Constant firstConstant = new Constant("%constant", timestampArray, firstValue);
        Constant secondConstant = new Constant("%constant", timestampArray, secondValue);
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        assertThat(thirdTimestamp).isEqualTo(firstTimestamp);
        // blocks wrapping the two values are not equal
        Block thirdValue = createTimestampsWithTimeZoneMillisBlock((long) thirdTimestamp);
        assertThat(thirdValue).isNotEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", timestampArray, thirdValue);
        // Constant operations wrapping them are also not equal since array of timestamp with time zone is compared by value (block)
        assertThat(thirdConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isNotEqualTo(firstConstant);

        // Two Constants using the same Block instance are equal
        Constant fourthConstant = new Constant("%constant", timestampArray, firstValue);
        assertThat(fourthConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(fourthConstant).isEqualTo(firstConstant);

        // two Blocks with different content are not equal
        Object fifthTimestamp = parseTimeWithTimeZone(3, "00:55:00.000 +00:00");
        Block fifthValue = createTimestampsWithTimeZoneMillisBlock((long) fifthTimestamp);
        Constant fifthConstant = new Constant("%constant", timestampArray, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testBooleanMap()
    {
        // map of boolean is compared by IDENTICAL
        MapType booleanMap = new MapType(BOOLEAN, BOOLEAN, TYPE_OPERATORS);
        assertThat(isOrContainsTypeComparedByValue(booleanMap)).isFalse();

        // two separate SqlMap with the same content are not equal
        SqlMap firstValue = new SqlMap(
                booleanMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(true, false),
                createBooleansBlock(true, false));
        SqlMap secondValue = new SqlMap(
                booleanMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(true, false),
                createBooleansBlock(true, false));
        assertThat(firstValue).isNotEqualTo(secondValue);
        // but Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", booleanMap, firstValue);
        Constant secondConstant = new Constant("%constant", booleanMap, secondValue);
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);

        // two SqlMap with the same content including nulls
        SqlMap thirdValue = new SqlMap(
                booleanMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(true, false),
                createBooleansBlock(true, null));
        SqlMap fourthValue = new SqlMap(
                booleanMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(true, false),
                createBooleansBlock(true, null));
        assertThat(thirdValue).isNotEqualTo(fourthValue);
        // but Constant operations wrapping them are equal. They are compared by IDENTICAL including nulls
        Constant thirdConstant = new Constant("%constant", booleanMap, thirdValue);
        Constant fourthConstant = new Constant("%constant", booleanMap, fourthValue);
        assertThat(thirdConstant.hashCode()).isEqualTo(fourthConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(fourthConstant);

        // two SqlMap with different content are not equal
        SqlMap fifthValue = new SqlMap(
                booleanMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(false),
                createBooleansBlock(true));
        assertThat(firstValue).isNotEqualTo(fifthValue);
        // and Constant operations wrapping them are not equal
        Constant fifthConstant = new Constant("%constant", booleanMap, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testTimestampMap()
    {
        // map of timestamp with time zone is compared by value (equals() method of the underlying SqlMap)
        MapType timestampMap = new MapType(BOOLEAN, TIMESTAMP_TZ_MILLIS, TYPE_OPERATORS);
        assertThat(isOrContainsTypeComparedByValue(timestampMap)).isTrue();

        // two different representations of the same time. They use different time zones.
        Object firstTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        Object secondTimestamp = parseTimeWithTimeZone(3, "08:01:00.000 +08:00");
        assertThat(firstTimestamp).isNotEqualTo(secondTimestamp);
        // SqlMaps wrapping the two values are not equal
        SqlMap firstValue = new SqlMap(
                timestampMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(false),
                createTimestampsWithTimeZoneMillisBlock((long) firstTimestamp));
        SqlMap secondValue = new SqlMap(
                timestampMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(false),
                createTimestampsWithTimeZoneMillisBlock((long) secondTimestamp));
        assertThat(firstValue).isNotEqualTo(secondValue);
        // Constant operations wrapping them are also not equal since map of timestamp with time zone is compared by value (SqlMap)
        Constant firstConstant = new Constant("%constant", timestampMap, firstValue);
        Constant secondConstant = new Constant("%constant", timestampMap, secondValue);
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        assertThat(thirdTimestamp).isEqualTo(firstTimestamp);
        // SqlMaps wrapping the two values are not equal
        SqlMap thirdValue = new SqlMap(
                timestampMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(false),
                createTimestampsWithTimeZoneMillisBlock((long) thirdTimestamp));
        assertThat(thirdValue).isNotEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", timestampMap, thirdValue);
        // Constant operations wrapping them are also not equal since map of timestamp with time zone is compared by value (SqlMap)
        assertThat(thirdConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isNotEqualTo(firstConstant);

        // Two Constants using the same SqlMap instance are equal
        Constant fourthConstant = new Constant("%constant", timestampMap, firstValue);
        assertThat(fourthConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(fourthConstant).isEqualTo(firstConstant);

        // two SqlMap with different content are not equal
        Object fifthTimestamp = parseTimeWithTimeZone(3, "00:55:00.000 +00:00");
        SqlMap fifthValue = new SqlMap(
                timestampMap,
                STRICT_NOT_DISTINCT_FROM,
                createBooleansBlock(false),
                createTimestampsWithTimeZoneMillisBlock((long) fifthTimestamp));
        Constant fifthConstant = new Constant("%constant", timestampMap, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testBooleanRow()
    {
        // row of boolean is compared by IDENTICAL
        RowType booleanRow = anonymousRow(BOOLEAN);
        assertThat(isOrContainsTypeComparedByValue(booleanRow)).isFalse();

        // two separate SqlRow with the same content are not equal
        SqlRow firstValue = new SqlRow(0, new Block[] {createBooleansBlock(true)});
        SqlRow secondValue = new SqlRow(0, new Block[] {createBooleansBlock(true)});
        assertThat(firstValue).isNotEqualTo(secondValue);
        // but Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", booleanRow, firstValue);
        Constant secondConstant = new Constant("%constant", booleanRow, secondValue);
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);

        // two SqlRow with different content including nulls
        SqlRow thirdValue = new SqlRow(0, new Block[] {createBooleansBlock(new Boolean[] {null})});
        SqlRow fourthValue = new SqlRow(0, new Block[] {createBooleansBlock(new Boolean[] {null})});
        assertThat(thirdValue).isNotEqualTo(fourthValue);
        // but Constant operations wrapping them are equal. They are compared by IDENTICAL including nulls
        Constant thirdConstant = new Constant("%constant", booleanRow, thirdValue);
        Constant fourthConstant = new Constant("%constant", booleanRow, fourthValue);
        assertThat(thirdConstant.hashCode()).isEqualTo(fourthConstant.hashCode());
        assertThat(thirdConstant).isEqualTo(fourthConstant);

        // two SqlRow with different content are not equal
        SqlRow fifthValue = new SqlRow(0, new Block[] {createBooleansBlock(false)});
        assertThat(firstValue).isNotEqualTo(fifthValue);
        // and Constant operations wrapping them are not equal
        Constant fifthConstant = new Constant("%constant", booleanRow, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testTimestampRow()
    {
        // row of timestamp with time zone is compared by value (equals() method of the underlying SqlRow)
        RowType timestampRow = anonymousRow(TIMESTAMP_TZ_MILLIS);
        assertThat(isOrContainsTypeComparedByValue(timestampRow)).isTrue();

        // two different representations of the same time. They use different time zones.
        Object firstTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        Object secondTimestamp = parseTimeWithTimeZone(3, "08:01:00.000 +08:00");
        assertThat(firstTimestamp).isNotEqualTo(secondTimestamp);
        // SqlRows wrapping the two values are not equal
        SqlRow firstValue = new SqlRow(0, new Block[] {createTimestampsWithTimeZoneMillisBlock((long) firstTimestamp)});
        SqlRow secondValue = new SqlRow(0, new Block[] {createTimestampsWithTimeZoneMillisBlock((long) secondTimestamp)});
        assertThat(firstValue).isNotEqualTo(secondValue);
        // Constant operations wrapping them are also not equal since row of timestamp with time zone is compared by value (SqlRow)
        Constant firstConstant = new Constant("%constant", timestampRow, firstValue);
        Constant secondConstant = new Constant("%constant", timestampRow, secondValue);
        assertThat(firstConstant.hashCode()).isNotEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isNotEqualTo(secondConstant);

        // create Constant operation using the same representation as the first one
        Object thirdTimestamp = parseTimeWithTimeZone(3, "00:01:00.000 +00:00");
        assertThat(thirdTimestamp).isEqualTo(firstTimestamp);
        // SqlRows wrapping the two values are not equal
        SqlRow thirdValue = new SqlRow(0, new Block[] {createTimestampsWithTimeZoneMillisBlock((long) thirdTimestamp)});
        assertThat(thirdValue).isNotEqualTo(firstValue);
        Constant thirdConstant = new Constant("%constant", timestampRow, thirdValue);
        // Constant operations wrapping them are also not equal since row of timestamp with time zone is compared by value (SqlRow)
        assertThat(thirdConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(thirdConstant).isNotEqualTo(firstConstant);

        // Two Constants using the same SqlRow instance are equal
        Constant fourthConstant = new Constant("%constant", timestampRow, firstValue);
        assertThat(fourthConstant.hashCode()).isEqualTo(firstConstant.hashCode());
        assertThat(fourthConstant).isEqualTo(firstConstant);

        // two SqlRow with different content are not equal
        Object fifthTimestamp = parseTimeWithTimeZone(3, "00:55:00.000 +00:00");
        SqlRow fifthValue = new SqlRow(0, new Block[] {createTimestampsWithTimeZoneMillisBlock((long) fifthTimestamp)});
        Constant fifthConstant = new Constant("%constant", timestampRow, fifthValue);
        assertThat(fifthConstant.hashCode()).isNotEqualTo(firstConstant.hashCode());
        assertThat(fifthConstant).isNotEqualTo(firstConstant);
    }

    @Test
    public void testNestedStructuralType()
    {
        MapType nestedType = new MapType(BOOLEAN, new ArrayType(anonymousRow(BOOLEAN, new MapType(BOOLEAN, new ArrayType(TIMESTAMP_TZ_MILLIS), TYPE_OPERATORS))), TYPE_OPERATORS);
        assertThat(isOrContainsTypeComparedByValue(nestedType)).isTrue();
    }

    @Test
    public void testStructuralTypeWithNaN()
    {
        ArrayType doubleArray = new ArrayType(DOUBLE);

        // two different NaN representations
        double firstNaN = longBitsToDouble(0x7FF0000000000001L);
        double secondNaN = longBitsToDouble(0x7FFFFFFFFFFFFFFFL);
        Block firstValue = createDoublesBlock(firstNaN, null);
        Block secondValue = createDoublesBlock(secondNaN, null);
        // Constant operations wrapping them are equal
        Constant firstConstant = new Constant("%constant", doubleArray, firstValue);
        Constant secondConstant = new Constant("%constant", doubleArray, secondValue);
        assertThat(firstConstant.hashCode()).isEqualTo(secondConstant.hashCode());
        assertThat(firstConstant).isEqualTo(secondConstant);
    }
}
