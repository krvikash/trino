/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.benchmark;

import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithQueryId;
import one.profiler.AsyncProfiler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public final class BenchmarkClickBench
{
    private BenchmarkClickBench() {}

    private static final Logger log = Logger.get(BenchmarkClickBench.class);

    static void main(String[] args)
    {
        System.exit(new CommandLine(new Benchmark()).execute(args));
    }

    @Command(name = "benchmark")
    public static class Benchmark
            implements Callable<Void>
    {
        @Option(names = {"-m", "--mode"}, description = "Execution mode", required = true)
        ExecutionMode executionMode;

        @Option(names = {"-w", "--warmup"}, description = "Number of warmup runs for each query")
        int warmup = 3;

        @Option(names = {"-r", "--runs"}, description = "Number of benchmark runs for each query")
        int runs = 5;

        @Option(names = {"-q", "--query"}, description = "A specific query to run [1-43]")
        Integer query;

        @Option(names = {"-p", "--profile"}, description = "Output directory for async-profiler flamegraphs")
        Path profileOutputDir;

        @Override
        public Void call()
                throws Exception
        {
            Path dataLocation = dataLocation();
            if (!isDirectory(dataLocation)) {
                throw new IllegalStateException("Data location %s does not exist. Run testing/benchmark-data/setup.sh first.".formatted(dataLocation));
            }

            AsyncProfiler profiler = profileOutputDir != null ? AsyncProfiler.getInstance() : null;
            if (profiler != null) {
                Files.createDirectories(profileOutputDir);
                log.info("Profiler output will be written to %s", profileOutputDir.toAbsolutePath());
            }

            try (DistributedQueryRunner runner = setup(executionMode)) {
                log.info("Running Trino at %s", runner.getCoordinator().getBaseUrl());
                log.info("Running benchmark %s warmup %s measured runs, reporting average.".formatted(warmup, runs));
                verifyDataset(runner);
                if (query != null) {
                    benchmarkQuery(runner, query, profiler);
                }
                else {
                    long sumOfAverages = 0;
                    for (int queryNumber = 1; queryNumber <= 43; queryNumber++) {
                        long averageMillis = benchmarkQuery(runner, queryNumber, profiler);
                        sumOfAverages += averageMillis;
                    }
                    System.out.println("ALL_QUERIES: %s ms".formatted(sumOfAverages));
                }
            }

            return null;
        }

        private void verifyDataset(DistributedQueryRunner runner)
        {
            assertThat(new QueryAssertions(runner).query("SELECT count(*) FROM hive.clickbench.hits"))
                    .matches("VALUES BIGINT '99997497'");
        }

        /**
         * Run benchmark query and return average query elapsed time as millis
         */
        private long benchmarkQuery(DistributedQueryRunner runner, int queryNumber, AsyncProfiler profiler)
                throws IOException
        {
            String query = readResource("sql/trino/clickbench/q%02d.sql".formatted(queryNumber))
                    .replace("${database}", "hive")
                    .replace("${schema}", "clickbench")
                    .trim()
                    .replaceFirst(";$", "");

            for (int i = 0; i < warmup; i++) {
                measureQueryTime(runner, query);
            }

            if (profiler != null) {
                profiler.execute("start,event=cpu");
            }

            List<Long> measurements = newArrayListWithExpectedSize(runs);
            for (int i = 0; i < runs; i++) {
                measurements.add(measureQueryTime(runner, query));
            }

            if (profiler != null) {
                Path outputFile = profileOutputDir.resolve("q%02d.html".formatted(queryNumber));
                profiler.execute("stop,file=%s".formatted(outputFile.toAbsolutePath()));
                log.info("Profiler output for q%02d written to %s", queryNumber, outputFile);
            }

            long averageMillis = (long) measurements.stream().mapToLong(Long::longValue).average().orElseThrow();
            System.out.println("q%02d: %s ms".formatted(queryNumber, averageMillis));
            return averageMillis;
        }

        /**
         * Run benchmark query and return average query elapsed time as millis
         */
        private long measureQueryTime(DistributedQueryRunner runner, String query)
        {
            MaterializedResultWithQueryId result = runner.executeWithQueryId(runner.getDefaultSession(), query);
            Duration elapsedTime = runner.getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats().getElapsedTime();
            return elapsedTime.toMillis();
        }
    }

    enum ExecutionMode
    {
        CPU, GPU,
    }

    private static DistributedQueryRunner setup(ExecutionMode executionMode)
            throws Exception
    {
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                .setWorkerCount(0) // single-node
                .setSkipTimezoneSetup(true);
        switch (executionMode) {
            case CPU -> builder.addExtraProperty("gpu-acceleration.enabled", "false");
            case GPU -> builder.addExtraProperty("gpu-acceleration.enabled", "true");
        }
        DistributedQueryRunner queryRunner = builder.build();

        // Create schema
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS hive.clickbench");

        // Create table with ClickBench schema pointing to external location
        {
            String dataLocation = dataLocation().toAbsolutePath().normalize().toString();
            queryRunner.execute(format("""
                                       CREATE TABLE hive.clickbench.hits (
                                           WatchID BIGINT,
                                           JavaEnable SMALLINT,
                                           Title VARCHAR,
                                           GoodEvent SMALLINT,
                                           EventTime TIMESTAMP(3),
                                           EventDate DATE,
                                           CounterID INTEGER,
                                           ClientIP INTEGER,
                                           RegionID INTEGER,
                                           UserID BIGINT,
                                           CounterClass SMALLINT,
                                           OS SMALLINT,
                                           UserAgent SMALLINT,
                                           URL VARCHAR,
                                           Referer VARCHAR,
                                           IsRefresh SMALLINT,
                                           RefererCategoryID SMALLINT,
                                           RefererRegionID INTEGER,
                                           URLCategoryID SMALLINT,
                                           URLRegionID INTEGER,
                                           ResolutionWidth SMALLINT,
                                           ResolutionHeight SMALLINT,
                                           ResolutionDepth SMALLINT,
                                           FlashMajor SMALLINT,
                                           FlashMinor SMALLINT,
                                           FlashMinor2 VARCHAR,
                                           NetMajor SMALLINT,
                                           NetMinor SMALLINT,
                                           UserAgentMajor SMALLINT,
                                           UserAgentMinor VARCHAR,
                                           CookieEnable SMALLINT,
                                           JavascriptEnable SMALLINT,
                                           IsMobile SMALLINT,
                                           MobilePhone SMALLINT,
                                           MobilePhoneModel VARCHAR,
                                           Params VARCHAR,
                                           IPNetworkID INTEGER,
                                           TraficSourceID SMALLINT,
                                           SearchEngineID SMALLINT,
                                           SearchPhrase VARCHAR,
                                           AdvEngineID SMALLINT,
                                           IsArtifical SMALLINT,
                                           WindowClientWidth SMALLINT,
                                           WindowClientHeight SMALLINT,
                                           ClientTimeZone SMALLINT,
                                           ClientEventTime TIMESTAMP(3),
                                           SilverlightVersion1 SMALLINT,
                                           SilverlightVersion2 SMALLINT,
                                           SilverlightVersion3 INTEGER,
                                           SilverlightVersion4 SMALLINT,
                                           PageCharset VARCHAR,
                                           CodeVersion INTEGER,
                                           IsLink SMALLINT,
                                           IsDownload SMALLINT,
                                           IsNotBounce SMALLINT,
                                           FUniqID BIGINT,
                                           OriginalURL VARCHAR,
                                           HID INTEGER,
                                           IsOldCounter SMALLINT,
                                           IsEvent SMALLINT,
                                           IsParameter SMALLINT,
                                           DontCountHits SMALLINT,
                                           WithHash SMALLINT,
                                           HitColor VARCHAR,
                                           LocalEventTime TIMESTAMP(3),
                                           Age SMALLINT,
                                           Sex SMALLINT,
                                           Income SMALLINT,
                                           Interests SMALLINT,
                                           Robotness SMALLINT,
                                           RemoteIP INTEGER,
                                           WindowName INTEGER,
                                           OpenerName INTEGER,
                                           HistoryLength SMALLINT,
                                           BrowserLanguage VARCHAR,
                                           BrowserCountry VARCHAR,
                                           SocialNetwork VARCHAR,
                                           SocialAction VARCHAR,
                                           HTTPError SMALLINT,
                                           SendTiming INTEGER,
                                           DNSTiming INTEGER,
                                           ConnectTiming INTEGER,
                                           ResponseStartTiming INTEGER,
                                           ResponseEndTiming INTEGER,
                                           FetchTiming INTEGER,
                                           SocialSourceNetworkID SMALLINT,
                                           SocialSourcePage VARCHAR,
                                           ParamPrice BIGINT,
                                           ParamOrderID VARCHAR,
                                           ParamCurrency VARCHAR,
                                           ParamCurrencyID SMALLINT,
                                           OpenstatServiceName VARCHAR,
                                           OpenstatCampaignID VARCHAR,
                                           OpenstatAdID VARCHAR,
                                           OpenstatSourceID VARCHAR,
                                           UTMSource VARCHAR,
                                           UTMMedium VARCHAR,
                                           UTMCampaign VARCHAR,
                                           UTMContent VARCHAR,
                                           UTMTerm VARCHAR,
                                           FromTag VARCHAR,
                                           HasGCLID SMALLINT,
                                           RefererHash BIGINT,
                                           URLHash BIGINT,
                                           CLID INTEGER
                                       )
                                       WITH (
                                           external_location = '%s',
                                           format = 'PARQUET'
                                       )
                                       """, dataLocation));
        }

        return queryRunner;
    }

    private static String readResource(String resourceName)
    {
        try {
            return Resources.toString(getResource(resourceName), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path dataLocation()
    {
        return findRepositoryRoot().resolve("testing/benchmark-data/clickbench/hive/hits");
    }

    private static Path findRepositoryRoot()
    {
        Path workingDirectory = Path.of("").toAbsolutePath();
        log.info("Current working directory: %s", workingDirectory);
        for (Path path = workingDirectory; path != null; path = path.getParent()) {
            if (isDirectory(path.resolve(".git"))) {
                return path;
            }
        }
        throw new RuntimeException("Failed to find repository root from " + workingDirectory);
    }
}
