/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling;

import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.stargate.buffer.data.client.DataPage;

import java.util.List;

public interface SpooledChunkReader
        extends AutoCloseable
{
    ListenableFuture<List<DataPage>> getDataPages(SpooledChunk spooledChunk);
}
