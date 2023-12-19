/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.testing;

import io.airlift.slice.Slice;
import io.starburst.stargate.buffer.data.client.DataPage;

import java.util.List;

record ClosedChunkData(List<DataPage> dataPages)
{
    public int getDataSize()
    {
        return dataPages.stream().map(DataPage::data).mapToInt(Slice::length).sum();
    }
}
