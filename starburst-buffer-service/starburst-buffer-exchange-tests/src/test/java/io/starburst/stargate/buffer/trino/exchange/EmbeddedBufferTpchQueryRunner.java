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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.DataSize;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;

public final class EmbeddedBufferTpchQueryRunner
{
    private static final Logger log = Logger.get(EmbeddedBufferTpchQueryRunner.class);

    private EmbeddedBufferTpchQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private int workerCount = 2;
        private boolean localDiskEnabled;
        private Optional<Path> localDiskDirectory = Optional.empty();
        private DataSize localDiskCapacity = DataSize.of(2, DataSize.Unit.GIGABYTE);
        private double memoryHighWatermark;
        private double memoryLowWatermark;
        private Optional<DataSize> chunkMemoryBudget = Optional.empty();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema("tiny")
                    .build());
        }

        public Builder withWorkerCount(int workerCount)
        {
            this.workerCount = workerCount;
            return this;
        }

        public Builder withLocalDisk()
        {
            this.localDiskEnabled = true;
            return this;
        }

        public Builder withLocalDiskCapacity(DataSize capacity)
        {
            this.localDiskCapacity = capacity;
            return this;
        }

        public Builder withLocalDiskDirectory(Path directory)
        {
            this.localDiskDirectory = Optional.of(directory);
            return this;
        }

        public Builder withMemoryHighWatermark(double watermark)
        {
            this.memoryHighWatermark = watermark;
            return this;
        }

        public Builder withMemoryLowWatermark(double watermark)
        {
            this.memoryLowWatermark = watermark;
            return this;
        }

        public Builder withChunkMemoryBudget(DataSize budget)
        {
            this.chunkMemoryBudget = Optional.of(budget);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            File spoolingDir = createTempDirectory("embedded_buffer_spooling").toFile();

            Map<String, String> extraProperties = new HashMap<>(FaultTolerantExecutionConnectorTestHelper.getExtraProperties());
            extraProperties.put("embedded-buffer-service-enabled", "true");
            extraProperties.put("buffer.testing.allow-local-spooling", "true");
            extraProperties.put("buffer.spooling.directory", "file://" + spoolingDir.getAbsolutePath());
            extraProperties.put("buffer.spooling.storage-driver", "TRINO_FS");
            extraProperties.put("buffer.spooling.local.location", "/");
            extraProperties.put("buffer.draining.min-duration", "5s");
            extraProperties.put("query.max-memory-per-node", "30%");
            extraProperties.put("query.executor-pool-size", "10");
            extraProperties.put("shutdown.grace-period", "1s");

            if (localDiskEnabled) {
                Path diskDir = localDiskDirectory.orElseGet(() -> {
                    try {
                        return createTempDirectory("embedded_buffer_disk_tier");
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                extraProperties.put("buffer.local-disk.enabled", "true");
                extraProperties.put("buffer.local-disk.directory", diskDir.toAbsolutePath().toString());
                extraProperties.put("buffer.local-disk.capacity", localDiskCapacity.toString());
                extraProperties.put("buffer.local-disk.routing.memory-high-watermark", Double.toString(memoryHighWatermark));
                extraProperties.put("buffer.local-disk.routing.memory-low-watermark", Double.toString(memoryLowWatermark));
                extraProperties.put("buffer.local-disk.testing.allow-directory-creation", "true");
            }
            chunkMemoryBudget.ifPresent(budget -> extraProperties.put("buffer.memory.chunks", budget.toString()));

            setExtraProperties(extraProperties);
            addCoordinatorProperty("node-scheduler.include-coordinator", "true");
            setWorkerCount(workerCount);
            withExchange("buffer", ImmutableMap.<String, String>builder()
                    .put("exchange.use-embedded-buffer-service", "true")
                    .put("exchange.sink-target-written-pages-count", "3")
                    .put("exchange.source-handle-target-chunks-count", "4")
                    .put("exchange.min-base-buffer-nodes-per-partition", "1")
                    .put("exchange.max-base-buffer-nodes-per-partition", "1")
                    .buildOrThrow());

            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.starburst.stargate.buffer", Level.DEBUG);
        DistributedQueryRunner queryRunner = builder()
                .withWorkerCount(1)
                .withChunkMemoryBudget(DataSize.of(16, DataSize.Unit.MEGABYTE))
                .build();
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
