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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

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

    private final CounterStatWithRate spooledDataSize;
    private final CounterStatWithRate spoolingFailures;
    private final CounterStatWithRate writtenDataSize;
    private final CounterStatWithRate readDataSize;

    @Inject
    public DataServerStatsLogger(DataServerStats dataServerStats)
    {
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        executorService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-stats-logger"));

        Ticker ticker = Ticker.systemTicker();
        this.spooledDataSize = new CounterStatWithRate(dataServerStats.getSpooledDataSize(), ticker);
        this.spoolingFailures = new CounterStatWithRate(dataServerStats.getSpoolingFailures(), ticker);
        this.writtenDataSize = new CounterStatWithRate(dataServerStats.getWrittenDataSize(), ticker);
        this.readDataSize = new CounterStatWithRate(dataServerStats.getReadDataSize(), ticker);
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
                "overloaded_add_data_pages_count: %s\n".formatted(dataServerStats.getOverloadedAddDataPagesCount().getTotalCount()) +
                "STATS END\n");
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    private static class CounterStatWithRate
    {
        private final CounterStat counterStat;
        private final Ticker ticker;

        @GuardedBy("this")
        private long lastUpdateTimeNanos;
        @GuardedBy("this")
        private long lastCounter;
        @GuardedBy("this")
        private double rate;

        public CounterStatWithRate(CounterStat counterStat, Ticker ticker)
        {
            this.counterStat = requireNonNull(counterStat, "counterStat is null");
            this.ticker = requireNonNull(ticker, "ticker is null");
        }

        public synchronized long getCounter()
        {
            return lastCounter;
        }

        public synchronized double getRate()
        {
            update(ticker.read(), counterStat.getTotalCount());
            return rate;
        }

        @GuardedBy("this")
        private void update(long currentTimeNanos, long counter)
        {
            if (lastUpdateTimeNanos != 0) {
                double timeDelta = (currentTimeNanos - lastUpdateTimeNanos) / 1_000_000_000d;
                long counterDelta = counter - lastCounter;
                rate = (double) counterDelta / timeDelta;
            }
            lastUpdateTimeNanos = currentTimeNanos;
            lastCounter = counter;
        }
    }
}
