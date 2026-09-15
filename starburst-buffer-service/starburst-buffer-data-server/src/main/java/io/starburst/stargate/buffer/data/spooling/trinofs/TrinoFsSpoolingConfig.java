/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.trinofs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

public class TrinoFsSpoolingConfig
{
    private int executorThreads = 50;
    private int deleteExecutorThreads = 10;

    @Min(1)
    public int getExecutorThreads()
    {
        return executorThreads;
    }

    @Config("spooling.trino-fs.executor-threads")
    @ConfigDescription("Size of the thread pool that executes blocking TrinoFileSystem read/write calls.")
    public TrinoFsSpoolingConfig setExecutorThreads(int executorThreads)
    {
        this.executorThreads = executorThreads;
        return this;
    }

    @Min(1)
    public int getDeleteExecutorThreads()
    {
        return deleteExecutorThreads;
    }

    @Config("spooling.trino-fs.delete-executor-threads")
    @ConfigDescription("Size of the thread pool that executes blocking TrinoFileSystem delete calls. Sized smaller than the read/write pool because deletes are best-effort cleanup and tolerate queueing.")
    public TrinoFsSpoolingConfig setDeleteExecutorThreads(int deleteExecutorThreads)
    {
        this.deleteExecutorThreads = deleteExecutorThreads;
        return this;
    }
}
