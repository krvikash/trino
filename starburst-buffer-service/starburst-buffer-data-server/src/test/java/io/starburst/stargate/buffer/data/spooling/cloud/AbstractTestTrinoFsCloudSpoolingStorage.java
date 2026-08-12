/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.cloud;

import io.starburst.stargate.buffer.data.client.DataApiConfig;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.client.spooling.trinofs.TrinoFsSpooledChunkReader;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.server.BufferNodeId;
import io.starburst.stargate.buffer.data.server.DataServerStats;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.MergedFileNameGenerator;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.trinofs.TrinoFsSpoolingConfig;
import io.starburst.stargate.buffer.data.spooling.trinofs.TrinoFsSpoolingStorage;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;

/**
 * Shared scaffolding for cloud-backed {@link TrinoFsSpoolingStorage} integration tests.
 * Concrete subclasses provision a real cloud {@link TrinoFileSystem}, set {@link #fileSystem}
 * and {@link #rootUri} in {@link #setupCloudEnvironment()}, and reclaim resources in
 * {@link #tearDownCloudEnvironment()}.
 *
 * <p>The base class then exercises the full {@link AbstractTestSpoolingStorage} matrix
 * (happy path, large chunk overwrite, many-chunk read/write, metadata file round-trip,
 * post-freeze guards) against the cloud-backed filesystem via {@link TrinoFsSpoolingStorage}
 * (write path) and {@link TrinoFsSpooledChunkReader} (read path).
 */
public abstract class AbstractTestTrinoFsCloudSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    protected TrinoFileSystem fileSystem;
    protected String rootUri;

    @Override
    @BeforeAll
    public void init()
    {
        setupCloudEnvironment();
        super.init();
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        try {
            super.destroy();
        }
        finally {
            try {
                deleteRoot();
            }
            finally {
                tearDownCloudEnvironment();
                fileSystem = null;
                rootUri = null;
            }
        }
    }

    protected abstract void setupCloudEnvironment();

    protected abstract void tearDownCloudEnvironment();

    /**
     * Best-effort cleanup of objects written under {@link #rootUri}. Concrete subclasses that
     * provision a fresh container/bucket per test can override with a no-op since the container
     * tear-down already wipes everything.
     */
    protected void deleteRoot()
            throws IOException
    {
        if (fileSystem != null && rootUri != null) {
            fileSystem.deleteDirectory(Location.of(rootUri));
        }
    }

    @Override
    protected final SpoolingStorage createSpoolingStorage()
    {
        return new TrinoFsSpoolingStorage(
                new BufferNodeId(0L),
                new SpoolingDirectoryConfig().setSpoolingDirectory(rootUri),
                new MergedFileNameGenerator(),
                new DataServerStats(),
                fileSystem,
                newDirectExecutorService(),
                newDirectExecutorService(),
                new TrinoFsSpoolingConfig(),
                newTimeoutExecutor());
    }

    private static ScheduledExecutorService newTimeoutExecutor()
    {
        return new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("test-timeout-%s"));
    }

    @Override
    protected final SpooledChunkReader createSpooledChunkReader()
    {
        return new TrinoFsSpooledChunkReader(
                new DataApiConfig(),
                fileSystem,
                newDirectExecutorService());
    }
}
