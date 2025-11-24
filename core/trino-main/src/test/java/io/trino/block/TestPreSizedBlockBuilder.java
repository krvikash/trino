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
package io.trino.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.PreSizedBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPreSizedBlockBuilder
{
    @Test
    void testIntegerFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = INTEGER.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        INTEGER.writeLong(preSizedBlockBuilder, 10);
        INTEGER.writeLong(blockBuilder, 10);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        INTEGER.writeLong(preSizedBlockBuilder, 20);
        INTEGER.writeLong(blockBuilder, 20);
        INTEGER.writeLong(preSizedBlockBuilder, 20);
        INTEGER.writeLong(blockBuilder, 20);

        assertBlockEquals(INTEGER, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(INTEGER);
    }

    @Test
    void testBigintFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = BIGINT.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        BIGINT.writeLong(preSizedBlockBuilder, 100L);
        BIGINT.writeLong(blockBuilder, 100L);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        BIGINT.writeLong(preSizedBlockBuilder, 200L);
        BIGINT.writeLong(blockBuilder, 200L);
        BIGINT.writeLong(preSizedBlockBuilder, 300L);
        BIGINT.writeLong(blockBuilder, 300L);

        assertBlockEquals(BIGINT, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(BIGINT);
    }

    @Test
    void testDoubleFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = DOUBLE.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        DOUBLE.writeDouble(preSizedBlockBuilder, 1.5);
        DOUBLE.writeDouble(blockBuilder, 1.5);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        DOUBLE.writeDouble(preSizedBlockBuilder, 2.5);
        DOUBLE.writeDouble(blockBuilder, 2.5);
        DOUBLE.writeDouble(preSizedBlockBuilder, 3.5);
        DOUBLE.writeDouble(blockBuilder, 3.5);

        assertBlockEquals(DOUBLE, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(DOUBLE);
    }

    @Test
    void testSmallintFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = SMALLINT.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = SMALLINT.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        SMALLINT.writeLong(preSizedBlockBuilder, 10);
        SMALLINT.writeLong(blockBuilder, 10);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        SMALLINT.writeLong(preSizedBlockBuilder, 20);
        SMALLINT.writeLong(blockBuilder, 20);
        SMALLINT.writeLong(preSizedBlockBuilder, 30);
        SMALLINT.writeLong(blockBuilder, 30);

        assertBlockEquals(SMALLINT, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(SMALLINT);
    }

    @Test
    void testTinyintFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = TINYINT.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = TINYINT.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        TINYINT.writeLong(preSizedBlockBuilder, 10);
        TINYINT.writeLong(blockBuilder, 10);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        TINYINT.writeLong(preSizedBlockBuilder, 20);
        TINYINT.writeLong(blockBuilder, 20);
        TINYINT.writeLong(preSizedBlockBuilder, 30);
        TINYINT.writeLong(blockBuilder, 30);

        assertBlockEquals(TINYINT, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(TINYINT);
    }

    @Test
    void testBooleanFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = BOOLEAN.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        BOOLEAN.writeBoolean(preSizedBlockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, true);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        BOOLEAN.writeBoolean(preSizedBlockBuilder, false);
        BOOLEAN.writeBoolean(blockBuilder, false);
        BOOLEAN.writeBoolean(preSizedBlockBuilder, true);
        BOOLEAN.writeBoolean(blockBuilder, true);

        assertBlockEquals(BOOLEAN, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(BOOLEAN);
    }

    @Test
    void testDateFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = DATE.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        DATE.writeLong(preSizedBlockBuilder, 18000);
        DATE.writeLong(blockBuilder, 18000);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        DATE.writeLong(preSizedBlockBuilder, 18001);
        DATE.writeLong(blockBuilder, 18001);
        DATE.writeLong(preSizedBlockBuilder, 18002);
        DATE.writeLong(blockBuilder, 18002);

        assertBlockEquals(DATE, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(DATE);
    }

    @Test
    void testRealFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = REAL.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        REAL.writeLong(preSizedBlockBuilder, Float.floatToIntBits(1.5f));
        REAL.writeLong(blockBuilder, Float.floatToIntBits(1.5f));

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        REAL.writeLong(preSizedBlockBuilder, Float.floatToIntBits(2.5f));
        REAL.writeLong(blockBuilder, Float.floatToIntBits(2.5f));
        REAL.writeLong(preSizedBlockBuilder, Float.floatToIntBits(3.5f));
        REAL.writeLong(blockBuilder, Float.floatToIntBits(3.5f));

        assertBlockEquals(REAL, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(REAL);
    }

    @Test
    void testUuidFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = UUID.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = UUID.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        Slice input = javaUuidToTrinoUuid(randomUUID());
        UUID.writeSlice(preSizedBlockBuilder, input);
        UUID.writeSlice(blockBuilder, input);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        input = javaUuidToTrinoUuid(randomUUID());
        UUID.writeSlice(preSizedBlockBuilder, input);
        UUID.writeSlice(blockBuilder, input);
        input = javaUuidToTrinoUuid(randomUUID());
        UUID.writeSlice(preSizedBlockBuilder, input);
        UUID.writeSlice(blockBuilder, input);

        assertBlockEquals(UUID, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(UUID);
    }

    @Test
    void testVarcharFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = VARCHAR.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        VARCHAR.writeSlice(preSizedBlockBuilder, Slices.utf8Slice("hello"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("hello"));

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        VARCHAR.writeSlice(preSizedBlockBuilder, Slices.utf8Slice("world"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("world"));
        VARCHAR.writeSlice(preSizedBlockBuilder, Slices.utf8Slice("test"));
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("test"));

        assertBlockEquals(VARCHAR, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(VARCHAR);
    }

    @Test
    void testLongTimestampFixedSizeBlockBuilder()
    {
        PreSizedBlockBuilder preSizedBlockBuilder = TIMESTAMP_NANOS.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createBlockBuilder(null, 6);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        LongTimestamp timestamp = new LongTimestamp(1000000L, 123000);
        TIMESTAMP_NANOS.writeObject(preSizedBlockBuilder, timestamp);
        TIMESTAMP_NANOS.writeObject(blockBuilder, timestamp);

        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        timestamp = new LongTimestamp(2000000L, 789000);
        TIMESTAMP_NANOS.writeObject(preSizedBlockBuilder, timestamp);
        TIMESTAMP_NANOS.writeObject(blockBuilder, timestamp);
        timestamp = new LongTimestamp(3000000L, 345000);
        TIMESTAMP_NANOS.writeObject(preSizedBlockBuilder, timestamp);
        TIMESTAMP_NANOS.writeObject(blockBuilder, timestamp);

        assertBlockEquals(TIMESTAMP_NANOS, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyPreSizedBlockBuilder(TIMESTAMP_NANOS);
    }

    @Test
    void testArrayTypeFixedSizeBlockBuilder()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        PreSizedBlockBuilder preSizedBlockBuilder = arrayType.createPreSizedBlockBuilder(6);
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 6);

        // Null array
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        // Array with elements [10, 20, 30]
        Block array = longArrayBlock(10, 20, 30);
        arrayType.writeObject(preSizedBlockBuilder, array);
        arrayType.writeObject(blockBuilder, array);

        // More nulls
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        blockBuilder.appendNull();

        // Array with elements [40, 50]
        array = longArrayBlock(40, 50);
        arrayType.writeObject(preSizedBlockBuilder, array);
        arrayType.writeObject(blockBuilder, array);

        // Empty array []
        array = longArrayBlock();
        arrayType.writeObject(preSizedBlockBuilder, array);
        arrayType.writeObject(blockBuilder, array);

        assertBlockEquals(arrayType, preSizedBlockBuilder.build(), blockBuilder.build());

        verifyEmptyBlock(arrayType);
        verifyAllNullsBlock(arrayType);
        verifyValueBlockAppend(arrayType);
        verifyNewBlockBuilderLike(arrayType);
    }

    private static void verifyPreSizedBlockBuilder(Type type)
    {
        verifyEmptyBlock(type);
        verifyAllNullsBlock(type);
        verifyOverSizedBuilder(type);
        verifyValueBlockAppend(type);
        verifyNewBlockBuilderLike(type);
    }

    private static void verifyAllNullsBlock(Type type)
    {
        PreSizedBlockBuilder preSizedBlockBuilder = type.createPreSizedBlockBuilder(3);

        preSizedBlockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();

        Block actualBlock = preSizedBlockBuilder.build();
        assertThat(actualBlock).isInstanceOf(RunLengthEncodedBlock.class);
        assertBlockEquals(type, actualBlock, RunLengthEncodedBlock.create(type, null, 3));
    }

    private static void verifyEmptyBlock(Type type)
    {
        PreSizedBlockBuilder preSizedBlockBuilder = type.createPreSizedBlockBuilder(0);
        assertBlockEquals(type, preSizedBlockBuilder.build(), type.createBlockBuilder(null, 0).build());
    }

    private static void verifyOverSizedBuilder(Type type)
    {
        PreSizedBlockBuilder preSizedBlockBuilder = type.createPreSizedBlockBuilder(3);

        preSizedBlockBuilder.appendNull();
        preSizedBlockBuilder.appendNull();

        Block actualBlock = preSizedBlockBuilder.build();
        assertBlockEquals(
                type,
                actualBlock,
                type.createBlockBuilder(null, 2).appendNull().appendNull().build());
    }

    private static void verifyValueBlockAppend(Type type)
    {
        ValueBlock block = createRandomBlockForType(type, 100, 0.2f);
        PreSizedBlockBuilder preSizedBlockBuilder = type.createPreSizedBlockBuilder(250);

        for (int i = 0; i < block.getPositionCount(); i++) {
            preSizedBlockBuilder.append(block, i);
        }

        assertBlockEquals(type, preSizedBlockBuilder.build(), block);
    }

    private static void verifyNewBlockBuilderLike(Type type)
    {
        PreSizedBlockBuilder preSizedBlockBuilder = type.createPreSizedBlockBuilder(5);
        preSizedBlockBuilder.appendNull();
        PreSizedBlockBuilder newBuilder = preSizedBlockBuilder.newBlockBuilderLike(2);
        assertBlockEquals(type, newBuilder.build(), type.createBlockBuilder(null, 2).build());

        newBuilder.appendNull();
        assertBlockEquals(type, newBuilder.build(), type.createBlockBuilder(null, 2).appendNull().build());
    }

    private static Block longArrayBlock(long... values)
    {
        return new LongArrayBlock(values.length, Optional.empty(), values);
    }
}
