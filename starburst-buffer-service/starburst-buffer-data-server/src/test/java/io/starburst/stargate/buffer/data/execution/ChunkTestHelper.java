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
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.DataPage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;

import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.readSerializedPages;
import static org.assertj.core.api.Assertions.assertThat;

public final class ChunkTestHelper
{
    private ChunkTestHelper() {}

    public static void verifyChunkData(ChunkDataLease chunkData, DataPage... values)
    {
        List<Slice> chunkSlices = switch (chunkData) {
            case MemoryChunkDataLease memoryLease -> memoryLease.getChunkSlices();
            case DiskChunkDataLease ignored -> throw new UnsupportedOperationException("disk chunk lease not supported in test helper");
        };
        long checksum = chunkData.getChecksum();
        int numDataPages = chunkData.getNumDataPages();

        SliceOutput sliceOutput = Slices.allocate(chunkSlices.stream().mapToInt(Slice::length).sum()).getOutput();
        chunkSlices.forEach(sliceOutput::writeBytes);

        List<DataPage> dataPages = ImmutableList.copyOf(readSerializedPages(sliceOutput.getUnderlyingSlice().getInput()));
        assertThat(calculateChecksum(dataPages)).isEqualTo(checksum);
        assertThat(dataPages.size()).isEqualTo(numDataPages);

        assertThat(dataPages).containsExactlyInAnyOrder(values);

        chunkData.release();
    }

    public static ChunkDataLease toChunkDataLease(List<DataPage> dataPages)
    {
        int length = dataPages.stream().mapToInt(dataPage -> dataPage.data().length() + DATA_PAGE_HEADER_SIZE).sum();
        Slice slice = Slices.allocate(length);
        SliceOutput sliceOutput = slice.getOutput();
        for (DataPage dataPage : dataPages) {
            sliceOutput.writeShort(dataPage.taskId());
            sliceOutput.writeByte(dataPage.attemptId());
            sliceOutput.writeInt(dataPage.data().length());
            sliceOutput.writeBytes(dataPage.data());
        }
        return new MemoryChunkDataLease(
                ImmutableList.of(slice),
                calculateChecksum(dataPages),
                dataPages.size(),
                () -> {});
    }

    public static DiskChunkDataLease diskChunkDataLease(List<DataPage> dataPages, Path file)
    {
        int length = dataPages.stream()
                .mapToInt(dp -> DATA_PAGE_HEADER_SIZE + dp.data().length())
                .sum();
        Slice slice = Slices.allocate(length);
        SliceOutput out = slice.getOutput();
        for (DataPage pages : dataPages) {
            out.writeShort(pages.taskId());
            out.writeByte(pages.attemptId());
            out.writeInt(pages.data().length());
            out.writeBytes(pages.data());
        }
        try (FileChannel writeChannel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = slice.toByteBuffer();
            while (buffer.hasRemaining()) {
                writeChannel.write(buffer);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to write disk chunk to " + file, e);
        }
        try {
            FileChannel readChannel = FileChannel.open(file, StandardOpenOption.READ);
            return new DiskChunkDataLease(file, readChannel, length, calculateChecksum(dataPages), dataPages.size(), () -> {
                try {
                    readChannel.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException("failed to open disk chunk for reading " + file, e);
        }
    }

    public static ChunkDataLease toChunkDataLease(Set<List<DataPage>> slicesOfDataPages)
    {
        ImmutableList.Builder<Slice> sliceBuilder = ImmutableList.builder();
        ImmutableList.Builder<DataPage> dataPageBuilder = ImmutableList.builder();
        for (List<DataPage> dataPages : slicesOfDataPages) {
            int length = dataPages.stream().mapToInt(dataPage -> dataPage.data().length() + DATA_PAGE_HEADER_SIZE).sum();
            Slice slice = Slices.allocate(length);
            SliceOutput sliceOutput = slice.getOutput();
            for (DataPage dataPage : dataPages) {
                sliceOutput.writeShort(dataPage.taskId());
                sliceOutput.writeByte(dataPage.attemptId());
                sliceOutput.writeInt(dataPage.data().length());
                sliceOutput.writeBytes(dataPage.data());
                dataPageBuilder.add(dataPage);
            }
            sliceBuilder.add(slice);
        }
        List<DataPage> allDataPages = dataPageBuilder.build();
        return new MemoryChunkDataLease(
                sliceBuilder.build(),
                calculateChecksum(allDataPages),
                allDataPages.size(),
                () -> {});
    }
}
