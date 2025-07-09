/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;

import java.util.List;

public class BlackholeSpooledChunkReader
        implements SpooledChunkReader
{
    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        return Futures.immediateFuture(ImmutableList.of());
    }

    @Override
    public void close()
            throws Exception {}
}
