/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import com.google.common.base.Ticker;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

// temporary measure to get assess to data server stats until we have means to put that in Datadog
public class DataServerStatsLogger
{
    private static final Logger log = Logger.get(DataServerStatsLogger.class);

    private static final long LOG_INTERVAL_SECONDS = 10; // also serves as rate computation window so it should not be to short

    private final ScheduledExecutorService executorService;
    private final DataServerStats dataServerStats;

    private final CounterWithRate spooledDataSize;
    private final CounterWithRate spoolingFailures;
    private final CounterWithRate writtenDataSize;
    private final CounterWithRate readDataSize;

    @Inject
    public DataServerStatsLogger(DataServerStats dataServerStats)
    {
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        executorService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-stats-logger"));

        Ticker ticker = Ticker.systemTicker();
        this.spooledDataSize = new CounterWithRate(dataServerStats.getSpooledDataSize(), ticker);
        this.spoolingFailures = new CounterWithRate(dataServerStats.getSpoolingFailures(), ticker);
        this.writtenDataSize = new CounterWithRate(dataServerStats.getWrittenDataSize(), ticker);
        this.readDataSize = new CounterWithRate(dataServerStats.getReadDataSize(), ticker);
    }

    @PostConstruct
    public void start()
    {
        executorService.scheduleWithFixedDelay(this::logStatsSafe, 0, LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void logStatsSafe()
    {
        try {
            logStats();
        }
        catch (Exception e) {
            // catch exception so we are not unscheduled
            log.error(e, "Unexpected exception in logStats");
        }
    }

    private void logStats()
    {
        spooledDataSize.update();
        spoolingFailures.update();
        writtenDataSize.update();
        readDataSize.update();

        log.info("STATS START\n" +
                "total_memory: %s\n".formatted(succinctBytes(dataServerStats.getTotalMemoryInBytes())) +
                "free_memory: %s\n".formatted(succinctBytes(dataServerStats.getFreeMemoryInBytes())) +
                "non_poolable_memory: %s\n".formatted(succinctBytes(dataServerStats.getNonPoolableAllocatedMemoryInBytes())) +
                "exchanges: %s\n".formatted(dataServerStats.getTrackedExchanges()) +
                "open_chunks: %s\n".formatted(dataServerStats.getOpenChunks()) +
                "closed_chunks: %s\n".formatted(dataServerStats.getClosedChunks()) +
                "spooled_chunks: %s\n".formatted(dataServerStats.getSpooledChunks()) +
                "spooled_data_size: %s, %s/s\n".formatted(succinctBytes(spooledDataSize.getCounter()), succinctBytes((long) spooledDataSize.getRate())) +
                "spooling_failures: %s, %s/s\n".formatted(spoolingFailures.getCounter(), spoolingFailures.getRate()) +
                "spooled_chunk_size_distribution: %s\n".formatted(dataServerStats.getSpooledChunkSizeDistribution().snapshot()) +
                "written_data_size: %s, %s/s\n".formatted(succinctBytes(writtenDataSize.getCounter()), succinctBytes((long) writtenDataSize.getRate())) +
                "written_data_size_distribution: %s\n".formatted(dataServerStats.getWrittenDataSizeDistribution().snapshot()) +
                "written_data_size_per_partition_distribution: %s\n".formatted(dataServerStats.getWrittenDataSizePerPartitionDistribution().snapshot()) +
                "read_data_size: %s, %s/s\n".formatted(succinctBytes(readDataSize.getCounter()), succinctBytes((long) readDataSize.getRate())) +
                "read_data_size_distribution: %s\n".formatted(dataServerStats.getReadDataSizeDistribution().snapshot()) +
                "in_progress_add_data_pages_requests: %s\n".formatted(dataServerStats.getInProgressAddDataPagesRequests()) +
                "STATS END\n");
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }
}
