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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.starburst.stargate.buffer.data.client.ChunkHandle;
import io.starburst.stargate.buffer.data.client.DataPage;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
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
            Slice chunkSlice)
    {
        this.bufferNodeId = bufferNodeId;
        this.partitionId = partitionId;
        this.chunkId = chunkId;
        this.chunkData = new ChunkData(requireNonNull(chunkSlice, "chunkSlice is null"));
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public int getPartitionId()
    {
        return partitionId;
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

    public List<DataPage> readAll()
    {
        checkState(closed, "readAll() called on an open chunk");
        return ImmutableList.copyOf(chunkData.dataPageIterator());
    }

    public ChunkHandle getHandle()
    {
        checkState(closed, "getHandle() called on an open chunk");
        if (chunkHandle == null) {
            chunkHandle = new ChunkHandle(bufferNodeId, partitionId, chunkId, chunkData.dataSizeInBytes());
        }
        return chunkHandle;
    }

    public void close()
    {
        closed = true;
    }

    private static class ChunkData
    {
        private final SliceOutput sliceOutput;

        private int dataSizeInBytes;

        public ChunkData(Slice chunkSlice)
        {
            this.sliceOutput = chunkSlice.getOutput();
        }

        public void write(int taskId, int attemptId, Slice data)
        {
            int writableBytes = sliceOutput.writableBytes();
            int dataSize = data.length();
            int requiredStorageSize = DATA_PAGE_HEADER_SIZE + dataSize;
            checkArgument(writableBytes >= requiredStorageSize, "writableBytes %s less than requiredStorageSize %s", writableBytes, requiredStorageSize);

            sliceOutput.writeShort(taskId);
            sliceOutput.writeByte(attemptId);
            sliceOutput.writeInt(dataSize);
            sliceOutput.writeBytes(data);

            dataSizeInBytes += dataSize;
        }

        public boolean hasEnoughSpace(Slice data)
        {
            return sliceOutput.writableBytes() >= DATA_PAGE_HEADER_SIZE + data.length();
        }

        public int dataSizeInBytes()
        {
            return dataSizeInBytes;
        }

        public Iterator<DataPage> dataPageIterator()
        {
            SliceInput sliceInput = sliceOutput.slice().getInput();

            return new Iterator<>() {
                @Override
                public boolean hasNext()
                {
                    return sliceInput.isReadable();
                }

                @Override
                public DataPage next()
                {
                    int taskId = sliceInput.readShort(); // addDataPage() guarantees taskId is no more than 32767
                    int attemptId = sliceInput.readByte(); // addDataPage() guarantees attemptId is no more than 127
                    Slice data = sliceInput.readSlice(sliceInput.readInt());
                    return new DataPage(taskId, attemptId, data);
                }
            };
        }
    }
}
