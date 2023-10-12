/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;

import java.util.concurrent.atomic.AtomicLong;

import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.PATH_SEPARATOR;
import static io.starburst.stargate.buffer.data.spooling.SpoolingUtils.getPrefixedDirectoryName;

@ThreadSafe
public class MergedFileNameGenerator
{
    private final AtomicLong idSequence = new AtomicLong();

    @Inject
    public MergedFileNameGenerator()
    {
    }

    public String getNextMergedFileName(long bufferNodeId, String exchangeId)
    {
        long id = idSequence.getAndIncrement();
        return getPrefixedDirectoryName(bufferNodeId, exchangeId, id) + PATH_SEPARATOR + id;
    }
}
