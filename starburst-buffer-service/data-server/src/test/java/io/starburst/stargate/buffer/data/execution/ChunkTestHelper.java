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

import java.util.List;

import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.DATA_PAGE_HEADER_SIZE;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.readSerializedPages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ChunkTestHelper
{
    private ChunkTestHelper() {}

    public static void verifyChunkData(ChunkDataLease chunkData, DataPage... values)
    {
        List<Slice> chunkSlices = chunkData.chunkSlices();
        long checksum = chunkData.checksum();
        int numDataPages = chunkData.numDataPages();

        SliceOutput sliceOutput = Slices.allocate(chunkSlices.stream().mapToInt(Slice::length).sum()).getOutput();
        chunkSlices.forEach(sliceOutput::writeBytes);

        List<DataPage> dataPages = ImmutableList.copyOf(readSerializedPages(sliceOutput.getUnderlyingSlice().getInput()));
        assertEquals(checksum, calculateChecksum(dataPages));
        assertEquals(numDataPages, dataPages.size());

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
        return new ChunkDataLease(
                ImmutableList.of(slice),
                calculateChecksum(dataPages),
                dataPages.size(),
                () -> {});
    }
}
