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

import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.calculateChecksum;
import static io.starburst.stargate.buffer.data.client.PagesSerdeUtil.readSerializedPages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ChunkTestHelper
{
    private ChunkTestHelper() {}

    public static void verifyChunkData(Chunk.ChunkDataRepresentation chunkDataRepresentation, DataPage... values)
    {
        List<Slice> chunkSlices = chunkDataRepresentation.chunkSlices();
        int numDataPages = chunkDataRepresentation.numDataPages();
        long checksum = chunkDataRepresentation.checksum();

        SliceOutput sliceOutput = Slices.allocate(chunkSlices.stream().mapToInt(Slice::length).sum()).getOutput();
        chunkSlices.forEach(sliceOutput::writeBytes);

        List<DataPage> dataPages = ImmutableList.copyOf(readSerializedPages(sliceOutput.getUnderlyingSlice().getInput()));
        assertEquals(numDataPages, dataPages.size());
        assertEquals(checksum, calculateChecksum(dataPages));

        assertThat(dataPages).containsExactlyInAnyOrder(values);
    }
}
