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
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class SourceBenchmark
{
    private static final Logger log = Logger.get(SourceBenchmark.class);

    private SourceBenchmark() {}

    public static void main(String[] args)
    {
        DataSize totalDataSize = DataSize.of(32, GIGABYTE);
        List<SourceBenchmarkSetup> setups = ImmutableList.of(
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(256, KILOBYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(2, MEGABYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),

                // small page; test changes to other options
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(2, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(16, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 4, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 4, DataSize.of(32, MEGABYTE), DataSize.of(128, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 1, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(8, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 8, 1, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(1, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 4, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)),
                new SourceBenchmarkSetup(1, totalDataSize, DataSize.of(8, MEGABYTE), DataSize.of(32, KILOBYTE), 1, 1, DataSize.of(128, MEGABYTE), DataSize.of(512, MEGABYTE)));

        Map<SourceBenchmarkSetup, Optional<SourceBenchmarkResult>> results = new LinkedHashMap<>();
        for (SourceBenchmarkSetup setup : setups) {
            ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
            try {
                SourceBenchmarkDriver driver = new SourceBenchmarkDriver(setup, executor);
                try {
                    results.put(setup, Optional.of(driver.benchmark()));
                }
                catch (Exception e) {
                    log.error(e, "benchmark for %s failed", setup);
                    results.put(setup, Optional.empty());
                }
            }
            finally {
                executor.shutdownNow();
            }
        }
        log.info("");
        log.info("==============================");
        log.info("RESULTS:");
        for (Map.Entry<SourceBenchmarkSetup, Optional<SourceBenchmarkResult>> entry : results.entrySet()) {
            log.info("%s -> %s", entry.getKey(), entry.getValue().map(Object::toString).orElse("ERROR"));
        }
        log.info("==============================");
        log.info("");
    }
}
