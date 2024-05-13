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

import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class WarmupElementBlocks
{
    private static final Logger logger = Logger.get(WarmupElementBlocks.class);

    private final WarmupElementWriteMetadata metadata;
    private final int recordBufferSize;
    private final int chunkSize;

    private List<Block> blocks; // from different pages
    private int startOffsetInFirstBlock;
    private int positionCount; // accumulative
    private long logicalSizeInBytes; // accumulative
    private double logicalSizeFactor = 1;
    private boolean factorRecentlyUpdated;

    public WarmupElementBlocks(WarmupElementWriteMetadata metadata, int recordBufferSize, int chunkSize)
    {
        this.metadata = metadata;
        this.recordBufferSize = recordBufferSize;
        this.chunkSize = chunkSize;
        this.blocks = new ArrayList<>();
    }

    public boolean add(Block block)
    {
        blocks.add(block);
        positionCount += block.getPositionCount();
        logicalSizeInBytes += block.getLoadedBlock().getLogicalSizeInBytes();
        return isReady();
    }

    public boolean isReady()
    {
        return positionCount >= chunkSize ||
                logicalSizeFactor * logicalSizeInBytes >= recordBufferSize;
    }

    public boolean isEmpty()
    {
        return blocks.isEmpty();
    }

    public void dropProcessed(int blocksToDrop, int startOffsetInNextBlock)
    {
        logger.debug("Dropping %d blocks. Setting startOffsetInNextBlock=%d", blocksToDrop, startOffsetInNextBlock);
        checkArgument(blocksToDrop >= 0 && startOffsetInNextBlock >= 0,
                format("blocksToDrop and startOffsetInNextBlock can't be negative numbers. blocksToDrop=%d, startOffsetInNextBlock=%d",
                        blocksToDrop, startOffsetInNextBlock));
        int affectedBlocks = startOffsetInNextBlock > 0 ? blocksToDrop + 1 : blocksToDrop;
        checkArgument(affectedBlocks <= blocks.size(),
                format("Can't drop more blocks than existing / set start offset in a non existing block. block.size=%d, blocksToDrop=%d, startOffsetInNextBlock=%d",
                        blocks.size(), blocksToDrop, startOffsetInNextBlock));
        checkArgument(blocksToDrop > 0 || startOffsetInNextBlock >= startOffsetInFirstBlock,
                format("Can't drop 0 blocks while decreasing the offset. startOffsetInNextBlock=%d, startOffsetInFirstBlock=%d",
                        startOffsetInNextBlock, startOffsetInFirstBlock));
        if (startOffsetInNextBlock > 0) { // can't be part of the condition of checkArgument because blocks.get(blocksToDrop) will throw if blocksToDrop == blocks.size()
            checkArgument(startOffsetInNextBlock < blocks.get(blocksToDrop).getPositionCount(),
                    format("startOffsetInNextBlock must be smaller than positionCount. startOffsetInNextBlock=%d, positionCount=%d",
                            startOffsetInNextBlock, blocks.get(blocksToDrop).getPositionCount()));
        }

        for (int i = 0; i < blocksToDrop; i++) {
            if (i > 0 || startOffsetInFirstBlock == 0) { // To spare calculations, the first block might already been taken into account before this call
                logicalSizeInBytes -= blocks.get(i).getLoadedBlock().getLogicalSizeInBytes();
                positionCount -= blocks.get(i).getPositionCount();
            }
        }

        // To spare calculations, count partial block (if startOffsetInNextBlock > 0) in full
        // (we can't ignore it so isReady() won't return true before time)
        if (startOffsetInNextBlock > 0 &&
                (blocksToDrop > 0 || startOffsetInFirstBlock == 0)) { // only if the block wasn't taken into account before this call
            logicalSizeInBytes -= blocks.get(blocksToDrop).getLoadedBlock().getLogicalSizeInBytes();
            positionCount -= blocks.get(blocksToDrop).getPositionCount();
        }

        blocks = blocks.stream().skip(blocksToDrop)
                .collect(Collectors.toCollection(ArrayList::new));
        startOffsetInFirstBlock = startOffsetInNextBlock;
        factorRecentlyUpdated = false;

        if (blocks.isEmpty()) {
            checkState(logicalSizeInBytes == 0, "logicalSizeInBytes is non-zero although there are 0 blocks after drop");
            checkState(positionCount == 0, "positionCount is non-zero although there are 0 blocks after drop");
        }
        else {
            checkState(logicalSizeInBytes >= 0, "logicalSizeInBytes became negative after drop");
            checkState(positionCount >= 0, "positionCount became negative after drop");
        }
    }

    // This method is called in case we tried to write the blocks because the threshold was reached,
    // but in practice, the blocks were not enough to fill the buffer.
    // For example, at VariableWidthBlock, 5 bytes are added to each value's size (((Integer.BYTES + Byte.BYTES) * (long) positionCount))
    // while at VariableLengthStringBlockAppender, we add only 1 extra byte (att.recLen).
    // Since we want a generic solution, instead of calculating the actual size of each block type separately, we maintain a general factor
    // that will decrease the chances for it to happen again in the next iterations.
    // The factor's value is > 0 and <= 1.
    public void updateFactor(int notFlushedBytes)
    {
        if (notFlushedBytes == 0) {
            return; // the factor can't be 0
        }

        if (notFlushedBytes >= logicalSizeInBytes) {
            logger.warn("Expected notFlushedBytes to be less than logicalSizeInBytes. notFlushedBytes=%d, logicalSizeInBytes=%d",
                    notFlushedBytes, logicalSizeInBytes);
            return;
        }

        double newFactor = (0.99 * notFlushedBytes) / logicalSizeInBytes; // Remove 1% so non-flushing won't happen again with the exact same amount of bytes
        if (newFactor > logicalSizeFactor) {
            logger.warn("Expected new factor to be less than the existing one. newFactor=%f, notFlushedBytes=%d, logicalSizeInBytes=%d, logicalSizeFactor=%f",
                    newFactor, notFlushedBytes, logicalSizeInBytes, logicalSizeFactor);
            return;
        }

        logger.debug("Updating logicalSizeFactor from %f to %f", logicalSizeFactor, newFactor);
        logicalSizeFactor = newFactor;
        factorRecentlyUpdated = true;
    }

    public WarmupElementWriteMetadata getMetadata()
    {
        return metadata;
    }

    public List<Block> getBlocks()
    {
        return blocks;
    }

    public int getStartOffsetInFirstBlock()
    {
        return startOffsetInFirstBlock;
    }

    public boolean isFactorRecentlyUpdated()
    {
        return factorRecentlyUpdated;
    }

    @Override
    public String toString()
    {
        return "WarmupElementBlocks{" +
                ", warmupElementWriteMetadata=" + metadata +
                ", blocks.size=" + blocks.size() +
                ", startOffsetInFirstBlock=" + startOffsetInFirstBlock +
                ", positionCount=" + positionCount +
                ", logicalSizeInBytes=" + logicalSizeInBytes +
                ", factorRecentlyUpdated=" + factorRecentlyUpdated +
                '}';
    }
}
