/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling.trinofs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

public class TrinoFsClientConfig
{
    private int executorThreads = 50;

    @Min(1)
    public int getExecutorThreads()
    {
        return executorThreads;
    }

    @Config("trino-fs.executor-threads")
    @ConfigDescription("Size of the thread pool that executes blocking TrinoFileSystem reads when use-trino-fs=true.")
    public TrinoFsClientConfig setExecutorThreads(int executorThreads)
    {
        this.executorThreads = executorThreads;
        return this;
    }
}
