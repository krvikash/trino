/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.local;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunkReader;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;
import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsLocalSpooledChunkReader;
import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createTrinoFsLocalSpoolingStorage;

public class TestTrinoFsLocalSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    // @TempDir is per-method; AbstractTestSpoolingStorage wires the storage in @BeforeAll
    // (PER_CLASS), so the directory has to be created manually.
    private Path tempDir;

    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        try {
            tempDir = Files.createTempDirectory("trino-fs-spooling-test-");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return createTrinoFsLocalSpoolingStorage(tempDir);
    }

    @Override
    protected SpooledChunkReader createSpooledChunkReader()
    {
        return createTrinoFsLocalSpooledChunkReader(tempDir);
    }

    @AfterAll
    public void cleanupTempDir()
            throws IOException
    {
        if (tempDir != null) {
            MoreFiles.deleteRecursively(tempDir, RecursiveDeleteOption.ALLOW_INSECURE);
            tempDir = null;
        }
    }
}
