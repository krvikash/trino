/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import java.util.concurrent.atomic.AtomicLong;

public class DataPagesIdGenerator
{
    private final AtomicLong generator = new AtomicLong();

    public long nextId()
    {
        return generator.getAndIncrement();
    }
}
