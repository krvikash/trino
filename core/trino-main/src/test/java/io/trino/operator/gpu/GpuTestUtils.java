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

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;

import java.util.Optional;
import java.util.Random;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class GpuTestUtils
{
    private GpuTestUtils() {}

    public static Block createBlock(Type type, int positionsCount, NullsProvider nullsProvider)
    {
        return createBlock(type, positionsCount, nullsProvider, new Random(42));
    }

    public static Block createBlock(Type type, int positionsCount, NullsProvider nullsProvider, Random random)
    {
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        BlockBuilder builder = type.createBlockBuilder(null, positionsCount);

        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                writeRandomValue(builder, type, random);
            }
        }
        return builder.build();
    }

    public static Block createBigintBlock(int positionsCount, NullsProvider nullsProvider, long minValue, long maxValue)
    {
        Random random = new Random(42);
        Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
        BlockBuilder builder = BIGINT.createBlockBuilder(null, positionsCount);
        for (int i = 0; i < positionsCount; i++) {
            if (isNull.isPresent() && isNull.get()[i]) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, random.nextLong(minValue, maxValue));
            }
        }
        return builder.build();
    }

    private static void writeRandomValue(BlockBuilder builder, Type type, Random random)
    {
        if (type == BOOLEAN) {
            BOOLEAN.writeBoolean(builder, random.nextBoolean());
        }
        else if (type == TINYINT) {
            TINYINT.writeLong(builder, random.nextInt(256) - 128);
        }
        else if (type == SMALLINT) {
            SMALLINT.writeLong(builder, random.nextInt(65536) - 32768);
        }
        else if (type == INTEGER) {
            INTEGER.writeLong(builder, random.nextInt(-10000, 10001));
        }
        else if (type == BIGINT) {
            BIGINT.writeLong(builder, random.nextLong(-10000, 10001));
        }
        else if (type == REAL) {
            REAL.writeFloat(builder, random.nextFloat() * 1000 - 500);
        }
        else if (type == DOUBLE) {
            DOUBLE.writeDouble(builder, random.nextDouble() * 1000 - 500);
        }
        else if (type == VARCHAR) {
            int length = random.nextInt(21);
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char) random.nextInt(0, Character.MIN_SURROGATE - 1);
            }
            VARCHAR.writeSlice(builder, Slices.utf8Slice(new String(chars)));
        }
        else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
