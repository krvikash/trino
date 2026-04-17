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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.stream.Collectors.joining;
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
        int warmup = 5;

        @Option(names = {"-r", "--runs"}, description = "Number of benchmark runs for each query")
        int runs = 10;

        @Option(names = {"-q", "--query"}, description = "A specific query to run [1-43] (can be repeated)")
        List<Integer> queries = new ArrayList<>();

        @Option(names = {"-p", "--profile"}, description = "Output directory for async-profiler flamegraphs")
        Path profileOutputDir;

        @Override
        public Void call()
                throws Exception
        {
            validateDataLocation();

            AsyncProfiler profiler = profileOutputDir != null ? AsyncProfiler.getInstance() : null;
            if (profiler != null) {
                Files.createDirectories(profileOutputDir);
                log.info("Profiler output will be written to %s", profileOutputDir.toAbsolutePath());
            }

            try (DistributedQueryRunner runner = setup(executionMode, false)) {
                log.info("Running Trino at %s", runner.getCoordinator().getBaseUrl());
                log.info("Running benchmark %s warmup %s measured runs, reporting average.".formatted(warmup, runs));
                verifyDataset(runner);
                if (!queries.isEmpty()) {
                    for (int query : queries) {
                        benchmarkQuery(runner, query, profiler);
                    }
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

        private void validateDataLocation()
                throws Exception
        {
            Path dataLocation = dataLocation();
            if (isDirectory(dataLocation)) {
                try (Stream<Path> list = Files.list(dataLocation)) {
                    String expected =
                            """
                            1014583318 20260416_150241_00003_a9apw_b591f863-9eda-4061-b7a8-89b18df8476d
                            1020501479 20260416_150241_00003_a9apw_071794c2-1005-4fd0-8d49-75454c1fa60c
                            1020647314 20260416_150241_00003_a9apw_6e12a868-fc9a-4e4c-93b1-05e35153d87c
                            1022069875 20260416_150241_00003_a9apw_7f66273d-04bf-453a-8fcf-837afdcb66aa
                            1022522724 20260416_150241_00003_a9apw_6540fd1f-95c0-4a45-bf46-33800e131107
                            1023090425 20260416_150241_00003_a9apw_a59f41e7-be3f-4c81-b07c-61eb737537cc
                            1023591135 20260416_150241_00003_a9apw_6a775878-68f4-42cf-a004-79f15af5ce59
                            1023619270 20260416_150241_00003_a9apw_357c2792-a24f-4380-bc71-6e0839828e3a
                            1023658000 20260416_150241_00003_a9apw_26259fab-d339-462d-a2f9-c94167f7c968
                            1023930602 20260416_150241_00003_a9apw_6be847fb-971d-4008-99ec-cad9618fcafc
                            1025297521 20260416_150241_00003_a9apw_f68d55cf-8a2c-4b11-90a1-880a05aa272f
                            1026335151 20260416_150241_00003_a9apw_97319857-11c5-4940-a6c5-d3df046f72e4
                            1026887707 20260416_150241_00003_a9apw_9c43df2c-d23f-48e1-ba3e-a7a5654499c4
                            1027407624 20260416_150241_00003_a9apw_b2a3199f-e96d-4309-9d77-90993a6470fe
                            1029638300 20260416_150241_00003_a9apw_327b7d9b-92ff-4a8a-80cd-a2f931f6981d
                            663112786 20260416_150241_00003_a9apw_28d665e2-325b-4bee-ad7c-4840739ffe5c
                            """;
                    String listing = list
                            .map(p -> "%s %s\n".formatted(size(p), p.getFileName()))
                            .sorted()
                            .collect(joining(""));
                    if (listing.equals(expected)) {
                        return;
                    }
                }
            }
            throw new IllegalStateException("Data location %s does not exist or is not up to date. Run testing/benchmark-data/hydrate.sh first.".formatted(dataLocation));
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
        CPU,
        GPU,
        GPU_TS,
    }

    private static DistributedQueryRunner setup(ExecutionMode executionMode, boolean bind8080)
            throws Exception
    {
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                .setWorkerCount(0) // single-node
                .addExtraProperty("query.max-memory-per-node", "6GB")
                .setSkipTimezoneSetup(true);
        switch (executionMode) {
            case CPU -> builder.addExtraProperty("gpu-acceleration.enabled", "false");
            case GPU -> builder.addExtraProperty("gpu-acceleration.enabled", "true");
            case GPU_TS -> builder
                    .addExtraProperty("gpu-acceleration.enabled", "true")
                    .addExtraProperty("gpu-acceleration.table-scan-enabled", "true");
        }
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
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

    static Path dataLocation()
    {
        return findRepositoryRoot().resolve("testing/benchmark-data/clickbench/hive/hits");
    }

    private static Path findRepositoryRoot()
    {
        Path workingDirectory = Path.of("").toAbsolutePath();
        for (Path path = workingDirectory; path != null; path = path.getParent()) {
            if (isDirectory(path.resolve(".git"))) {
                return path;
            }
        }
        throw new RuntimeException("Failed to find repository root from " + workingDirectory);
    }

    private static long size(Path p)
    {
        try {
            return Files.size(p);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class CpuRunner
    {
        static void main()
                throws Exception
        {
            DistributedQueryRunner queryRunner = setup(ExecutionMode.CPU, true);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static class GpuRunner
    {
        static void main()
                throws Exception
        {
            DistributedQueryRunner queryRunner = setup(ExecutionMode.GPU, true);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
