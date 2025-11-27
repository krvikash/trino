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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDictionaryAdaptiveBlockEncoding
{
    private final Block dictionary = buildTestDictionary();

    private BlockEncodingSerde blockEncodingSerde;

    @BeforeAll
    public void setup()
    {
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(new FeaturesConfig(), new BlockEncodingSimdSupport(true));
        assertThat(blockEncodingManager.getBlockEncodingByBlockClass(DictionaryBlock.class))
                .isInstanceOf(DictionaryAdaptiveBlockEncoding.class);
        blockEncodingSerde = new InternalBlockEncodingSerde(blockEncodingManager, TESTING_TYPE_MANAGER);
    }

    @Test
    public void testRoundTrip()
    {
        int positionCount = 40;

        // build ids
        int[] ids = new int[positionCount];
        for (int i = 0; i < 40; i++) {
            ids[i] = i % 4;
        }

        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionaryBlock);
    }

    @Test
    public void testNonSequentialDictionaryUnnest()
    {
        int[] ids = new int[] {3, 2, 1, 0};
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 4));
    }

    @Test
    public void testNonSequentialDictionaryUnnestWithGaps()
    {
        int[] ids = new int[] {3, 2, 0};
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertThat(actualBlock).isInstanceOf(VariableWidthBlock.class);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 3));
    }

    @Test
    public void testSequentialDictionaryUnnest()
    {
        int[] ids = new int[] {0, 1, 2, 3};
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertThat(actualBlock).isInstanceOf(VariableWidthBlock.class);
        assertBlockEquals(VARCHAR, actualBlock, dictionary.getPositions(ids, 0, 4));
    }

    @Test
    public void testIdsInRandomOrder()
    {
        int positionCount = 1000;

        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionary.getPositionCount();
        }

        int offset = 3; // this is to verify that offset is respected
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids)
                .getRegion(offset, positionCount - offset);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionaryBlock);
    }

    @Test
    public void testRleFriendlyIds()
    {
        int positionCount = 1000;

        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionary.getPositionCount();
        }
        Arrays.sort(ids);

        int offset = 3; // this is to verify that offset is respected
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids)
                .getRegion(offset, positionCount - offset);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionaryBlock);
    }

    @Test
    public void testDeltaFriendlyIds()
    {
        int positionCount = 1000;

        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, positionCount);
        int[] ids = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            VARCHAR.writeString(dictionaryBuilder, "string" + i);
            ids[i] = i;
        }
        ids[ids.length - 1] = ids[ids.length - 2]; // ensure at least one repeat to prevent conversion into a variable-width block.
        Block dictionary = dictionaryBuilder.build();

        int offset = 3; // this is to verify that offset is respected
        DictionaryBlock dictionaryBlock = (DictionaryBlock) DictionaryBlock.create(ids.length, dictionary, ids)
                .getRegion(offset, positionCount - offset);

        Block actualBlock = roundTripBlock(dictionaryBlock);
        assertBlockEquals(VARCHAR, actualBlock, dictionaryBlock);
    }

    protected Block roundTripBlock(Block block)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, block);
        return blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
    }

    private static Block buildTestDictionary()
    {
        // build dictionary
        BlockBuilder dictionaryBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(dictionaryBuilder, "alice");
        VARCHAR.writeString(dictionaryBuilder, "bob");
        VARCHAR.writeString(dictionaryBuilder, "charlie");
        VARCHAR.writeString(dictionaryBuilder, "dave");
        return dictionaryBuilder.build();
    }
}
