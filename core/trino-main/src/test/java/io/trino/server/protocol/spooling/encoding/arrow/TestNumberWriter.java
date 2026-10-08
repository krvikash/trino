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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TrinoNumber;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.type.NumberType.NUMBER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestNumberWriter
{
    private static final int POSITION_COUNT = 100;

    @Test
    void testEstimateCoversInfinity()
    {
        BlockBuilder blockBuilder = NUMBER.createBlockBuilder(null, POSITION_COUNT);
        for (int i = 0; i < POSITION_COUNT; i++) {
            NUMBER.writeObject(blockBuilder, TrinoNumber.from(new TrinoNumber.Infinity(i % 2 == 0)));
        }

        assertEstimateCoversText(blockBuilder.build(), "-Infinity");
    }

    @Test
    void testEstimateCoversMaxPrecision()
    {
        // 200 digits, the maximum precision, in the fewest magnitude bytes (83), with the largest scale,
        // so the text is in scientific notation with a 5-digit exponent: the longest text per stored byte
        BigDecimal value = new BigDecimal(BigInteger.TEN.pow(199).add(BigInteger.ONE).negate(), 16383);
        BlockBuilder blockBuilder = NUMBER.createBlockBuilder(null, POSITION_COUNT);
        for (int i = 0; i < POSITION_COUNT; i++) {
            NUMBER.writeObject(blockBuilder, TrinoNumber.from(value));
        }

        assertEstimateCoversText(blockBuilder.build(), value.toString());
        assertThat(value.toString()).hasSize(209);
    }

    private static void assertEstimateCoversText(Block block, String firstValue)
    {
        try (BufferAllocator allocator = new RootAllocator();
                VarCharVector vector = new VarCharVector("number", allocator)) {
            NumberWriter writer = new NumberWriter(vector);
            writer.initialize(block);
            writer.write(block);

            assertThat(new String(vector.get(0), UTF_8)).isEqualTo(firstValue);
            assertThat(writer.estimatedDataBufferSizeInBytes(block)).isGreaterThanOrEqualTo(vector.sizeOfValueBuffer());
        }
    }
}
