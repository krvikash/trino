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

import io.airlift.slice.DynamicSliceOutput;
import io.trino.FeaturesConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestVariableWidthAdaptiveBlockEncoding
        extends BaseBlockEncodingTest<String>
{
    @Override
    protected BlockEncodingSerde createBlockEncodingSerde()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(new FeaturesConfig(), new BlockEncodingSimdSupport(true));
        assertThat(blockEncodingManager.getBlockEncodingByBlockClass(VariableWidthBlock.class))
                .isInstanceOf(VariableWidthAdaptiveBlockEncoding.class);
        return new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }

    @Override
    protected Type getType()
    {
        return VARCHAR;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, String value)
    {
        VARCHAR.writeString(blockBuilder, value);
    }

    @Override
    protected String randomValue(Random random)
    {
        char[] value = new char[random.nextInt(16)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @Test
    public void testUnicode()
    {
        roundTrip(
                "\u0000",
                "Ní hé lá na gaoithe lá na scolb",
                "لولا اختلاف النظر، لبارت السلع",
                "△△▿▿◁▷◁▷BA",
                "Something in ASCII, latin ÿ, some İ and I, geometry ▦ and finally an emoji \uD83D\uDE0D");
    }

    @Test
    public void testRandomLengths()
    {
        int positionCount = 1000;
        BlockBuilder blockBuilder = getType().createBlockBuilder(null, positionCount);
        Random random = new Random(1234);

        for (int i = 0; i < positionCount; i++) {
            int length = random.nextInt(100);
            String value = randomValue(random, length);
            write(blockBuilder, value);
        }

        testRoundTrip(blockBuilder.buildValueBlock());
    }

    @Test
    public void testRleFriendlyIds()
    {
        int positionCount = 1000;
        BlockBuilder blockBuilder = getType().createBlockBuilder(null, positionCount);
        Random random = new Random(1234);

        for (int i = 0; i < positionCount; i++) {
            int length = i / 100 + 1;
            String value = randomValue(random, length);
            write(blockBuilder, value);
        }

        testRoundTrip(blockBuilder.buildValueBlock());
    }

    @Test
    public void testDeltaFriendlyIds()
    {
        int positionCount = 1000;
        BlockBuilder blockBuilder = getType().createBlockBuilder(null, positionCount);
        Random random = new Random(1234);

        for (int i = 0; i < positionCount; i++) {
            int length = i + 1;
            String value = randomValue(random, length);
            write(blockBuilder, value);
        }

        testRoundTrip(blockBuilder.buildValueBlock());
    }

    private String randomValue(Random random, int length)
    {
        char[] value = new char[length];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    private void testRoundTrip(Block block)
    {
        int positionCount = block.getPositionCount();

        int offset = 3; // this is to verify that offset is respected
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block.getRegion(offset, positionCount - offset);

        BlockEncodingSerde blockEncodingSerde = createBlockEncodingSerde();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, variableWidthBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(getType(), actualBlock, variableWidthBlock);
    }
}
