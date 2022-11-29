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
import io.airlift.stats.CounterStat;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

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
    private final Ticker ticker = Ticker.systemTicker();

    private final CounterWithRate spooledDataSize;
    private final CounterWithRate spoolingFailures;
    private final CounterWithRate unspooledDataSize;
    private final CounterWithRate unspoolingFailures;
    private final CounterWithRate writtenDataSize;
    private final CounterWithRate readDataSize;

    @Inject
    public DataServerStatsLogger(DataServerStats dataServerStats)
    {
        this.dataServerStats = requireNonNull(dataServerStats, "dataServerStats is null");
        executorService = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("data-server-stats-logger"));

        this.spooledDataSize = new CounterWithRate(dataServerStats.getSpooledDataSize(), ticker);
        this.spoolingFailures = new CounterWithRate(dataServerStats.getSpoolingFailures(), ticker);
        this.unspooledDataSize = new CounterWithRate(dataServerStats.getUnspooledDataSize(), ticker);
        this.unspoolingFailures = new CounterWithRate(dataServerStats.getUnspoolingFailures(), ticker);
        this.writtenDataSize = new CounterWithRate(dataServerStats.getWrittenDataSize(), ticker);
        this.readDataSize = new CounterWithRate(dataServerStats.getReadDataSize(), ticker);
    }

    @PostConstruct
    public void start()
    {
        executorService.scheduleWithFixedDelay(this::logStats, 0, LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private synchronized void logStats()
    {
        spooledDataSize.update();
        spoolingFailures.update();
        unspooledDataSize.update();
        unspoolingFailures.update();
        writtenDataSize.update();
        readDataSize.update();

        StringBuilder logBuilder = new StringBuilder();
        logBuilder.append("STATS START\n");
        logBuilder.append("total_memory: %s\n".formatted(succinctBytes(dataServerStats.getTotalMemoryInBytes())));
        logBuilder.append("free_memory: %s\n".formatted(succinctBytes(dataServerStats.getFreeMemoryInBytes())));
        logBuilder.append("exchanges: %s\n".formatted(dataServerStats.getTrackedExchanges()));
        logBuilder.append("open_chunks: %s\n".formatted(dataServerStats.getOpenChunks()));
        logBuilder.append("closed_chunks: %s\n".formatted(dataServerStats.getClosedChunks()));
        logBuilder.append("spooled_chunks: %s\n".formatted(dataServerStats.getSpooledChunks()));
        logBuilder.append("spooled_data_size: %s, %s/s\n".formatted(succinctBytes(spooledDataSize.getCounter()), succinctBytes((long) spooledDataSize.getRate())));
        logBuilder.append("spooling_failures: %s, %s/s\n".formatted(spoolingFailures.getCounter(), spoolingFailures.getRate()));
        logBuilder.append("spooled_chunk_size_distribution: %s\n".formatted(dataServerStats.getSpooledChunkSizeDistribution().snapshot()));
        logBuilder.append("unspooled_data_size: %s, %s/s\n".formatted(succinctBytes(unspooledDataSize.getCounter()), succinctBytes((long) unspooledDataSize.getRate())));
        logBuilder.append("unspooling_failures: %s, %s/s\n".formatted(unspoolingFailures.getCounter(), unspoolingFailures.getRate()));
        logBuilder.append("unspooled_chunk_size_distribution: %s\n".formatted(dataServerStats.getUnspooledChunkSizeDistribution().snapshot()));
        logBuilder.append("written_data_size: %s, %s/s\n".formatted(succinctBytes(writtenDataSize.getCounter()), succinctBytes((long) writtenDataSize.getRate())));
        logBuilder.append("read_data_size: %s, %s/s\n".formatted(succinctBytes(readDataSize.getCounter()), succinctBytes((long) readDataSize.getRate())));
        logBuilder.append("STATS END\n");
        log.info(logBuilder.toString());
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    private static class CounterWithRate
    {
        private final LongSupplier counterSupplier;
        private final Ticker ticker;

        private long lastUpdateTimeNanos;
        private long lastCounter;
        private double rate;

        public CounterWithRate(CounterStat counterStat, Ticker ticker)
        {
            this(counterStat::getTotalCount, ticker);
        }

        public CounterWithRate(LongSupplier counterSupplier, Ticker ticker)
        {
            this.counterSupplier = counterSupplier;
            this.ticker = ticker;
        }

        public void update()
        {
            update(ticker.read(), counterSupplier.getAsLong());
        }

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

        public long getCounter()
        {
            return lastCounter;
        }

        public double getRate()
        {
            return rate;
        }
    }
}
