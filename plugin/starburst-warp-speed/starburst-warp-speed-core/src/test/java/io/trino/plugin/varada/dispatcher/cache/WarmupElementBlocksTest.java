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
package io.trino.plugin.varada.dispatcher.cache;

import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WarmupElementBlocksTest
{
    @Test
    public void testReadyOnChunkSize()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block block = mockBlock(chunkSize, recordBufferSize - 1);

        assertThat(warmupElementBlocks.add(block)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.isEmpty()).isFalse();

        warmupElementBlocks.dropProcessed(0, 5);
        assertThat(warmupElementBlocks.isReady()).isFalse();
        assertThat(warmupElementBlocks.isEmpty()).isFalse();

        warmupElementBlocks.dropProcessed(1, 0);
        assertThat(warmupElementBlocks.isReady()).isFalse();
        assertThat(warmupElementBlocks.isEmpty()).isTrue();
    }

    @Test
    public void testReadyOnRecordBufferSize()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block block = mockBlock(chunkSize - 1, recordBufferSize);

        warmupElementBlocks.add(block);
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.isEmpty()).isFalse();

        // when removing a single record - should count the whole block as removed
        warmupElementBlocks.dropProcessed(0, 1);
        assertThat(warmupElementBlocks.isReady()).isFalse();
        assertThat(warmupElementBlocks.isEmpty()).isFalse();

        warmupElementBlocks.dropProcessed(1, 0);
        assertThat(warmupElementBlocks.isReady()).isFalse();
        assertThat(warmupElementBlocks.isEmpty()).isTrue();
    }

    @Test
    public void testExtraBlockAfterReady()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block bigEnoughBlock = mockBlock(chunkSize - 1, recordBufferSize);
        Block anExtraBlock = mockBlock(chunkSize - 1, recordBufferSize);

        // add
        assertThat(warmupElementBlocks.add(bigEnoughBlock)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(1);
        assertThat(warmupElementBlocks.add(anExtraBlock)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(2);

        // drop
        warmupElementBlocks.dropProcessed(0, 1);
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(2);
        assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(1);
        warmupElementBlocks.dropProcessed(1, 0);
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlocks()
    {
        int numberOfBlocks = 10;

        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        int recordsPerBlock = 4;
        int blockNeededToBeReady = (chunkSize / recordsPerBlock) + 1;

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);

        // add
        for (int i = 0; i < numberOfBlocks; i++) {
            Block block = mockBlock(recordsPerBlock, 30);
            boolean expectedToBeReady = warmupElementBlocks.getBlocks().size() + 1 >= blockNeededToBeReady; // +1 because we haven't added the block yet
            assertThat(warmupElementBlocks.add(block)).isEqualTo(expectedToBeReady);
            assertThat(warmupElementBlocks.isReady()).isEqualTo(expectedToBeReady);
        }
        assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(numberOfBlocks);
        assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(0);

        // drop
        int expectedBlocks = 10;
        int blocksToDropEachIteration = 2;
        int offset = 3;
        while (warmupElementBlocks.getBlocks().size() > blocksToDropEachIteration + 1) {
            warmupElementBlocks.dropProcessed(blocksToDropEachIteration, offset);
            expectedBlocks -= blocksToDropEachIteration;
            assertThat(warmupElementBlocks.getBlocks().size()).isEqualTo(expectedBlocks);
            assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(offset);
            boolean expectedToBeReady = warmupElementBlocks.getBlocks().size() >= blockNeededToBeReady + 1; // +1 because offset > 0 so an extra block will be counted on each drop
            assertThat(warmupElementBlocks.isReady()).isEqualTo(expectedToBeReady);
        }
    }

    @Test
    public void testAdvanceOffsets()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block block = mockBlock(chunkSize, recordBufferSize);
        assertThat(warmupElementBlocks.add(block)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();

        for (int i = 1; i < chunkSize; i++) {
            warmupElementBlocks.dropProcessed(0, i);
            assertThat(warmupElementBlocks.isEmpty()).isFalse();
            assertThat(warmupElementBlocks.isReady()).isFalse();
        }
    }

    @Test
    public void testInvalidInputOnDrop()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block block = mockBlock(5, 50);
        assertThat(warmupElementBlocks.add(block)).isFalse();

        // negative input
        assertThrows(RuntimeException.class, () -> warmupElementBlocks.dropProcessed(-1, 0));
        assertThrows(RuntimeException.class, () -> warmupElementBlocks.dropProcessed(0, -1));

        // drop too many blocks
        assertThrows(RuntimeException.class, () -> warmupElementBlocks.dropProcessed(2, 0));

        // offset too large
        assertThrows(RuntimeException.class, () -> warmupElementBlocks.dropProcessed(0, block.getPositionCount()));

        // move offset backwards
        warmupElementBlocks.dropProcessed(0, 2);
        assertThrows(RuntimeException.class, () -> warmupElementBlocks.dropProcessed(0, 1));
    }

    @Test
    public void testReadinessWithFactor()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        Block block = mockBlock(chunkSize - 1, recordBufferSize);

        warmupElementBlocks.add(block);
        assertThat(warmupElementBlocks.isReady()).isTrue();

        warmupElementBlocks.updateFactor(recordBufferSize - 1);
        assertThat(warmupElementBlocks.isReady()).isFalse();
    }

    @Test
    public void testInvalidInputOnUpdateFactor()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;

        // Factor can't be larger than 1
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        warmupElementBlocks.updateFactor(recordBufferSize + 1);
        warmupElementBlocks.dropProcessed(1, 0); // remove the first block
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        assertThat(warmupElementBlocks.isReady()).isTrue(); // the second block should make it ready because factor should remain 1

        // Can't enlarge the factor
        warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        warmupElementBlocks.updateFactor(recordBufferSize / 2);
        warmupElementBlocks.updateFactor(recordBufferSize - 2);
        warmupElementBlocks.dropProcessed(1, 0); // remove the first block
        warmupElementBlocks.add(mockBlock(1, recordBufferSize * 2 - 1));
        assertThat(warmupElementBlocks.isReady()).isFalse(); // the second block should not make it ready because the factor should remain 0.5*recordBufferSize

        // 0 should be ignored
        warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        warmupElementBlocks.updateFactor(0);
        warmupElementBlocks.dropProcessed(1, 0); // remove the first block
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        assertThat(warmupElementBlocks.isReady()).isTrue();  // if the factor is 0 then ready will return false

        // Update factor before adding any block should be ignored (recordBufferSize is 0)
        warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        warmupElementBlocks.updateFactor(1);
        warmupElementBlocks.add(mockBlock(1, recordBufferSize));
        assertThat(warmupElementBlocks.isReady()).isTrue();
        warmupElementBlocks.dropProcessed(1, 0);
        warmupElementBlocks.add(mockBlock(1, recordBufferSize - 1));
        assertThat(warmupElementBlocks.isReady()).isFalse();
    }

    @Test
    public void testFactorRecentlyUpdated()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int recordBufferSize = 100;
        int chunkSize = 10;

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, recordBufferSize, chunkSize);
        warmupElementBlocks.add(mockBlock(chunkSize, recordBufferSize));

        assertThat(warmupElementBlocks.isFactorRecentlyUpdated()).isFalse();
        warmupElementBlocks.updateFactor(recordBufferSize / 2);
        assertThat(warmupElementBlocks.isFactorRecentlyUpdated()).isTrue();
        warmupElementBlocks.dropProcessed(0, chunkSize / 2);
        assertThat(warmupElementBlocks.isFactorRecentlyUpdated()).isFalse();
    }

    private Block mockBlock(int positionCount, long logicalSizeInBytes)
    {
        Block block = mock(IntArrayBlock.class);
        when(block.getLoadedBlock()).thenReturn(block);
        when(block.getPositionCount()).thenReturn(positionCount);
        when(block.getLogicalSizeInBytes()).thenReturn(logicalSizeInBytes);
        return block;
    }
}
