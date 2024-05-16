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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WarmupElementBlocksTest
{
    @Test
    public void testReadyOnChunkSize()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, chunkSize);
        Block block = mockBlock(chunkSize);

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
    public void testExtraBlockAfterReady()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, chunkSize);
        Block bigEnoughBlock = mockBlock(chunkSize);
        Block anExtraBlock = mockBlock(chunkSize);

        // add
        assertThat(warmupElementBlocks.add(bigEnoughBlock)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getSize()).isEqualTo(1);
        assertThat(warmupElementBlocks.add(anExtraBlock)).isTrue();
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getSize()).isEqualTo(2);

        // drop
        warmupElementBlocks.dropProcessed(0, 1);
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getSize()).isEqualTo(2);
        assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(1);
        warmupElementBlocks.dropProcessed(1, 0);
        assertThat(warmupElementBlocks.isReady()).isTrue();
        assertThat(warmupElementBlocks.getSize()).isEqualTo(1);
    }

    @Test
    public void testMultipleBlocks()
    {
        int numberOfBlocks = 10;

        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int chunkSize = 10;
        int recordsPerBlock = 4;
        int blockNeededToBeReady = (chunkSize / recordsPerBlock) + 1;

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, chunkSize);

        // add
        for (int i = 0; i < numberOfBlocks; i++) {
            Block block = mockBlock(recordsPerBlock);
            boolean expectedToBeReady = warmupElementBlocks.getSize() + 1 >= blockNeededToBeReady; // +1 because we haven't added the block yet
            assertThat(warmupElementBlocks.add(block)).isEqualTo(expectedToBeReady);
            assertThat(warmupElementBlocks.isReady()).isEqualTo(expectedToBeReady);
        }
        assertThat(warmupElementBlocks.getSize()).isEqualTo(numberOfBlocks);
        assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(0);

        // drop
        int expectedBlocks = 10;
        int blocksToDropEachIteration = 2;
        int offset = 3;
        while (warmupElementBlocks.getSize() > blocksToDropEachIteration + 1) {
            warmupElementBlocks.dropProcessed(blocksToDropEachIteration, offset);
            expectedBlocks -= blocksToDropEachIteration;
            assertThat(warmupElementBlocks.getSize()).isEqualTo(expectedBlocks);
            assertThat(warmupElementBlocks.getStartOffsetInFirstBlock()).isEqualTo(offset);
            boolean expectedToBeReady = warmupElementBlocks.getSize() >= blockNeededToBeReady + 1; // +1 because offset > 0 so an extra block will be counted on each drop
            assertThat(warmupElementBlocks.isReady()).isEqualTo(expectedToBeReady);
        }
    }

    @Test
    public void testAdvanceOffsets()
    {
        WarmupElementWriteMetadata metadata = mock(WarmupElementWriteMetadata.class);
        int chunkSize = 10;

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, chunkSize);
        Block block = mockBlock(chunkSize);
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
        int chunkSize = 10;
        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(metadata, chunkSize);
        Block block = mockBlock(5);
        assertThat(warmupElementBlocks.add(block)).isFalse();

        // negative input
        assertThatThrownBy(() -> warmupElementBlocks.dropProcessed(-1, 0)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> warmupElementBlocks.dropProcessed(0, -1)).isInstanceOf(RuntimeException.class);

        // drop too many blocks
        assertThatThrownBy(() -> warmupElementBlocks.dropProcessed(2, 0)).isInstanceOf(RuntimeException.class);

        // offset too large
        assertThatThrownBy(() -> warmupElementBlocks.dropProcessed(0, block.getPositionCount())).isInstanceOf(RuntimeException.class);

        // move offset backwards
        warmupElementBlocks.dropProcessed(0, 2);
        assertThatThrownBy(() -> warmupElementBlocks.dropProcessed(0, 1)).isInstanceOf(RuntimeException.class);
    }

    private Block mockBlock(int positionCount)
    {
        Block block = mock(IntArrayBlock.class);
        when(block.getLoadedBlock()).thenReturn(block);
        when(block.getPositionCount()).thenReturn(positionCount);
        return block;
    }
}
