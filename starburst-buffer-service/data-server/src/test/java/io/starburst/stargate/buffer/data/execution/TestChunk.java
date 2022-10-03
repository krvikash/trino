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
import io.airlift.slice.Slices;
import io.starburst.stargate.buffer.data.client.DataPage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestChunk
{
    @Test
    public void testHappyPath()
    {
        Chunk chunk = new Chunk(0L, 0, 0, Slices.allocate(35));

        List<DataPage> dataPages = ImmutableList.of(
                new DataPage(0, 0, utf8Slice("333")),
                new DataPage(0, 0, utf8Slice("4444")),
                new DataPage(1, 0, utf8Slice("55555")));
        DataPage extraDataPage = new DataPage(1, 0, utf8Slice("666666"));

        for (DataPage dataPage : dataPages) {
            assertTrue(chunk.hasEnoughSpace(dataPage.data()));
            chunk.write(dataPage.taskId(), dataPage.attemptId(), dataPage.data());
        }
        assertFalse(chunk.hasEnoughSpace(extraDataPage.data()));
        chunk.close();

        assertEquals(12, chunk.dataSizeInBytes());
        assertThat(chunk.readAll()).containsExactlyElementsOf(dataPages);
    }
}
