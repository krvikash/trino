/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.noop;

import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;

import java.util.List;

public class NoopSpooledChunkReader
        implements SpooledChunkReader
{
    @Override
    public ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close()
            throws Exception
    {
    }
}
