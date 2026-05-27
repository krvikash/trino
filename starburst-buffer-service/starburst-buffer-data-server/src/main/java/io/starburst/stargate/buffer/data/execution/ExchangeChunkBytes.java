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

import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class ExchangeChunkBytes
{
    private final ConcurrentHashMap<String, AtomicLong> bytesPerExchange = new ConcurrentHashMap<>();

    public long addAndGet(String exchangeId, int bytes)
    {
        return bytesPerExchange.computeIfAbsent(exchangeId, _ -> new AtomicLong()).addAndGet(bytes);
    }

    public void remove(String exchangeId)
    {
        bytesPerExchange.remove(exchangeId);
    }
}
