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

import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;

public class WarmupElementBlocks
{
    private static final int INSTANCE_SIZE = instanceSize(WarmupElementBlocks.class);

    private static final Logger logger = Logger.get(WarmupElementBlocks.class);

    private final WarmupElementWriteMetadata metadata;
    private final int chunkSize;

    private List<Block> blocks; // from different pages
    private int startOffsetInFirstBlock;
    private int positionCount; // accumulative
    private long logicalSizeInBytes; // accumulative

    public WarmupElementBlocks(WarmupElementWriteMetadata metadata, int chunkSize)
    {
        this.metadata = metadata;
        this.chunkSize = chunkSize;
        this.blocks = new ArrayList<>();
    }

    public synchronized boolean add(Block block)
    {
        blocks.add(block);
        positionCount += block.getPositionCount();
        logicalSizeInBytes += block.getLoadedBlock().getLogicalSizeInBytes();
        return isReady();
    }

    public synchronized boolean isReady()
    {
        return positionCount >= chunkSize;
    }

    public synchronized boolean isEmpty()
    {
        return blocks.isEmpty();
    }

    public synchronized void dropProcessed(int blocksToDrop, int startOffsetInNextBlock)
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

        if (blocks.isEmpty()) {
            checkState(logicalSizeInBytes == 0, "logicalSizeInBytes is non-zero although there are 0 blocks after drop");
            checkState(positionCount == 0, "positionCount is non-zero although there are 0 blocks after drop");
        }
        else {
            checkState(logicalSizeInBytes >= 0, "logicalSizeInBytes became negative after drop");
            checkState(positionCount >= 0, "positionCount became negative after drop");
        }
    }

    public WarmupElementWriteMetadata getMetadata()
    {
        return metadata;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + logicalSizeInBytes;
    }

    public int getStartOffsetInFirstBlock()
    {
        return startOffsetInFirstBlock;
    }

    public synchronized int getSize()
    {
        return blocks.size();
    }

    public Block get(int index)
    {
        return blocks.get(index);
    }

    @Override
    public String toString()
    {
        return "WarmupElementBlocks{" +
                ", blocks.size=" + blocks.size() +
                ", startOffsetInFirstBlock=" + startOffsetInFirstBlock +
                ", positionCount=" + positionCount +
                ", isReady=" + isReady() +
                ", chunkSize=" + chunkSize +
                ", logicalSizeInBytes=" + logicalSizeInBytes +
                '}';
    }
}
