/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.memory.MemoryAllocator;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.NO_CHECKSUM;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class Chunk
{
    private final long bufferNodeId;
    private final int partitionId;
    private final long chunkId;
    private final ChunkData chunkData;

    private boolean closed;
    private ChunkHandle chunkHandle;

    public Chunk(
            long bufferNodeId,
            int partitionId,
            long chunkId,
            MemoryAllocator memoryAllocator,
            int chunkMaxSizeInBytes,
            int chunkSliceSizeInBytes,
            boolean calculateDataPagesChecksum)
    {
        this.bufferNodeId = bufferNodeId;
        this.partitionId = partitionId;
        this.chunkId = chunkId;
        this.chunkData = new ChunkData(memoryAllocator, chunkMaxSizeInBytes, chunkSliceSizeInBytes, calculateDataPagesChecksum);
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public void write(int taskId, int attemptId, Slice data)
    {
        checkState(!closed, "write() called on a closed chunk");
        chunkData.write(taskId, attemptId, data);
    }

    public boolean hasEnoughSpace(Slice data)
    {
        checkState(!closed, "hasEnoughSpace() called on a closed chunk");
        return chunkData.hasEnoughSpace(data);
    }

    public int dataSizeInBytes()
    {
        checkState(closed, "dataSizeInBytes() called on an open chunk");
        return chunkData.dataSizeInBytes();
    }

    public ChunkDataHolder getChunkData()
    {
        checkState(closed, "getChunkData() called on an open chunk");
        return chunkData.get();
    }

    public ChunkHandle getHandle()
    {
        checkState(closed, "getHandle() called on an open chunk");
        if (chunkHandle == null) {
            chunkHandle = new ChunkHandle(bufferNodeId, partitionId, chunkId, chunkData.dataSizeInBytes());
        }
        return chunkHandle;
    }

    public void release()
    {
        chunkData.release();
    }

    public void close()
    {
        chunkData.close();
        closed = true;
    }

    private static class ChunkData
    {
        private final MemoryAllocator memoryAllocator;
        private final int chunkMaxSizeInBytes;
        private final int chunkSliceSizeInBytes;
        private final boolean calculateDataPagesChecksum;
        private final List<SliceOutput> completedSliceOutputs;

        private final XxHash64 hash = new XxHash64();

        private Slice headerSlice;
        private SliceOutput sliceOutput;
        private int numBytesWritten;
        private int dataSizeInBytes;
        private int numDataPages;

        public ChunkData(
                MemoryAllocator memoryAllocator,
                int chunkMaxSizeInBytes,
                int chunkSliceSizeInBytes,
                boolean calculateDataPagesChecksum)
        {
            checkArgument(chunkMaxSizeInBytes >= chunkSliceSizeInBytes && chunkMaxSizeInBytes % chunkSliceSizeInBytes == 0,
                    "chunkMaxSizeInBytes %s is not a multiple of chunkSliceSizeInBytes %s", chunkMaxSizeInBytes, chunkSliceSizeInBytes);
            this.memoryAllocator = requireNonNull(memoryAllocator, "memoryAllocator is null");
            this.chunkMaxSizeInBytes = chunkMaxSizeInBytes;
            this.chunkSliceSizeInBytes = chunkSliceSizeInBytes;
            this.calculateDataPagesChecksum = calculateDataPagesChecksum;
            this.completedSliceOutputs = new ArrayList<>(chunkMaxSizeInBytes / chunkSliceSizeInBytes);

            this.sliceOutput = createNewSliceOutput();
        }

        public void write(int taskId, int attemptId, Slice data)
        {
            int writableBytes = chunkMaxSizeInBytes - numBytesWritten;
            int dataSize = data.length();
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
            checkArgument(requiredStorageSize <= writableBytes, "requiredStorageSize %s larger than writableBytes %s", requiredStorageSize, writableBytes);

            if (sliceOutput.writableBytes() >= DATA_PAGE_HEADER_SIZE) {
                sliceOutput.writeShort(taskId);
                sliceOutput.writeByte(attemptId);
                sliceOutput.writeInt(dataSize);
            }
            else {
                if (headerSlice == null) {
                    // TODO: handle memory allocation failure
                    headerSlice = memoryAllocator.allocate(DATA_PAGE_HEADER_SIZE)
                            .orElseThrow(() -> new IllegalStateException("Failed to allocate %d bytes of memory".formatted(DATA_PAGE_HEADER_SIZE)));
                }
                SliceOutput headerSliceOutput = headerSlice.getOutput();
                headerSliceOutput.writeShort(taskId);
                headerSliceOutput.writeByte(attemptId);
                headerSliceOutput.writeInt(dataSize);
                writeData(headerSlice);
            }
            writeData(data);

            if (calculateDataPagesChecksum) {
                hash.update(data);
            }
            numDataPages++;
            numBytesWritten += requiredStorageSize;
            dataSizeInBytes += dataSize;
        }

        public boolean hasEnoughSpace(Slice data)
        {
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + data.length();
            int writableBytes = chunkMaxSizeInBytes - numBytesWritten;
            checkArgument(requiredStorageSize <= chunkMaxSizeInBytes, "requiredStorageSize %s larger than chunkMaxSizeInBytes %s", requiredStorageSize, chunkMaxSizeInBytes);
            return requiredStorageSize <= writableBytes;
        }

        public int dataSizeInBytes()
        {
            return dataSizeInBytes;
        }

        public ChunkDataHolder get()
        {
            List<Slice> chunkSlices = completedSliceOutputs.stream().map(SliceOutput::slice).collect(toImmutableList());
            if (!calculateDataPagesChecksum) {
                return new ChunkDataHolder(chunkSlices, NO_CHECKSUM, numDataPages);
            }

            long checksum = hash.hash();
            if (checksum == NO_CHECKSUM) {
                checksum++;
            }
            return new ChunkDataHolder(chunkSlices, checksum, numDataPages);
        }

        public void close()
        {
            if (headerSlice != null) {
                memoryAllocator.release(headerSlice);
                headerSlice = null;
            }
            if (sliceOutput != null) {
                completedSliceOutputs.add(sliceOutput);
                sliceOutput = null;
            }
        }

        public void release()
        {
            if (headerSlice != null) {
                memoryAllocator.release(headerSlice);
            }
            completedSliceOutputs.forEach(output -> memoryAllocator.release(output.getUnderlyingSlice()));
        }

        private SliceOutput createNewSliceOutput()
        {
            // TODO: handle memory allocation failure
            return memoryAllocator.allocate(chunkSliceSizeInBytes)
                    .orElseThrow(() -> new IllegalStateException("Failed to allocate %d bytes of memory".formatted(chunkSliceSizeInBytes)))
                    .getOutput();
        }

        private void writeData(Slice data)
        {
            int offset = 0;
            int dataLength = data.length();
            while (offset < dataLength) {
                if (!sliceOutput.isWritable()) {
                    completedSliceOutputs.add(sliceOutput);
                    sliceOutput = createNewSliceOutput();
                }
                int bytesToWrite = Math.min(dataLength - offset, sliceOutput.writableBytes());
                sliceOutput.writeBytes(data, offset, bytesToWrite);
                offset += bytesToWrite;
            }
        }
    }
}
