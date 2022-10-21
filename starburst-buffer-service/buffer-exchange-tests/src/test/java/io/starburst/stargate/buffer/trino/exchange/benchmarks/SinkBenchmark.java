/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.benchmarks;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class SinkBenchmark
{
    private static final Logger log = Logger.get(SinkBenchmark.class);

    private SinkBenchmark() {}

    public static void main(String[] args)
    {
        DataSize dataPerWriter = DataSize.of(4, DataSize.Unit.GIGABYTE);
        List<SinkBenchmarkSetup> setups = ImmutableList.of(
                new SinkBenchmarkSetup(8, 1, DataSize.of(32, DataSize.Unit.KILOBYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)),
                new SinkBenchmarkSetup(8, 100, DataSize.of(32, DataSize.Unit.KILOBYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)),
                new SinkBenchmarkSetup(8, 1000, DataSize.of(32, DataSize.Unit.KILOBYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)),
                new SinkBenchmarkSetup(8, 1, DataSize.of(320, DataSize.Unit.KILOBYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)),
                new SinkBenchmarkSetup(8, 100, DataSize.of(320, DataSize.Unit.KILOBYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)),
                new SinkBenchmarkSetup(8, 1, DataSize.of(32, DataSize.Unit.MEGABYTE), dataPerWriter, DataSize.of(128, DataSize.Unit.MEGABYTE), DataSize.of(512, DataSize.Unit.MEGABYTE)));

        Map<SinkBenchmarkSetup, SinkBenchmarkResult> results = new LinkedHashMap<>();
        for (SinkBenchmarkSetup setup : setups) {
            ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
            try {
                SinkBenchmarkDriver driver = new SinkBenchmarkDriver(setup, executor);
                try {
                    results.put(setup, driver.benchmark());
                }
                catch (Exception e) {
                    log.error(e, "benchmark for %s failed", setup);
                }
            }
            finally {
                executor.shutdownNow();
            }
        }
        log.info("");
        log.info("==============================");
        log.info("RESULTS:");
        for (Map.Entry<SinkBenchmarkSetup, SinkBenchmarkResult> entry : results.entrySet()) {
            log.info("%s -> %s", entry.getKey(), entry.getValue());
        }
        log.info("==============================");
        log.info("");
    }
}
