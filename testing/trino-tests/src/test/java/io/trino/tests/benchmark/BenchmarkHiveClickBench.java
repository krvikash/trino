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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ClickBench entry point.
 */
public final class BenchmarkHiveClickBench
{
    private BenchmarkHiveClickBench() {}

    private static final Logger log = Logger.get(BenchmarkHiveClickBench.class);

    static final String HITS_COLUMNS =
            """
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
            """;

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new ClickBenchWorkload(), BenchmarkHiveClickBench.class));
    }

    static final class ClickBenchWorkload
            implements Workload
    {
        @Override
        public DataSize jvmHeapSize()
        {
            return DataSize.of(10, DataSize.Unit.GIGABYTE);
        }

        @Override
        public String name()
        {
            return "clickbench";
        }

        @Override
        public List<String> defaultQueries()
        {
            return ClickBench.allQueries();
        }

        @Override
        public String normalizeQuery(String query)
        {
            return ClickBench.normalizeQuery(query);
        }

        @Override
        public String readQuery(String query)
        {
            return ClickBench.readQuery(query, "hive", "clickbench");
        }

        @Override
        public String defaultDataLocation()
        {
            return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name()).toString();
        }

        @Override
        public void validateDataLocation(String dataLocation)
        {
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
            BenchmarkRunner.verifyDataListing(Path.of(dataLocation).resolve("hits"), "Run `testing/benchmark-data/hydrate.sh hive-clickbench` first.", expected);
        }

        @Override
        public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> fsCacheDirectory)
                throws Exception
        {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(testSessionBuilder().build());

            builder.setWorkerCount(0); // single-node
            if (bind8080) {
                builder.addCoordinatorProperty("http-server.http.port", "8080");
            }
            BenchmarkRunner.configureQueryRunner(builder, mode);

            DistributedQueryRunner queryRunner = builder.build();

            Map<String, String> hiveProperties = new HashMap<>();
            hiveProperties.put("hive.parquet.time-zone", "UTC");
            if (isRemote(dataLocation)) {
                hiveProperties.put("fs.s3.enabled", "true");
                BenchmarkRunner.applyFilesystemCache(queryRunner, hiveProperties, fsCacheDirectory);
            }
            else {
                hiveProperties.put("hive.metastore.catalog.dir", "local://" + queryRunner.getCoordinator().getBaseDataDir().resolve("clickbench-metastore").toAbsolutePath());
            }
            if (mode == BenchmarkRunner.ExecutionMode.GPU) {
                hiveProperties.put("hive.max-split-size", "512MB");
                hiveProperties.put("hive.parquet.max-split-size", "512MB");
            }

            // local:// routes reads through the native LocalFileSystem; root "/" so absolute data paths resolve.
            Path dataDir;
            if (isRemote(dataLocation)) {
                dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
            }
            else {
                dataDir = Path.of("/");
            }
            queryRunner.installPlugin(new TestingHivePlugin(dataDir));
            queryRunner.createCatalog("hive", "hive", hiveProperties);

            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS hive.clickbench");
            String hitsLocation = isRemote(dataLocation)
                    ? dataLocation
                    : "local://" + Path.of(dataLocation, "hits").toAbsolutePath();
            log.info("Creating hive.clickbench.hits at %s", hitsLocation);
            queryRunner.execute(format(
                    "CREATE TABLE hive.clickbench.hits (%s) WITH (external_location = '%s', format = 'PARQUET')",
                    HITS_COLUMNS,
                    hitsLocation));

            return queryRunner;
        }

        @Override
        public void verifyDataset(DistributedQueryRunner runner)
        {
            log.info("Verifying row count: hive.clickbench.hits");
            assertThat(new QueryAssertions(runner).query("SELECT count(*) FROM hive.clickbench.hits"))
                    .matches("VALUES BIGINT '99997497'");
        }

        @Override
        public List<String> tablesForStats()
        {
            // ClickBench is single-table denormalised — cost-based stats give no plan-shape
            // benefit, so don't require ANALYZE before benchmarking.
            return List.of();
        }

        @Override
        public String expectedResultResource(String query)
        {
            return "sql/trino/clickbench/results/%s.ndjson".formatted(query);
        }

        @Override
        public void generateData(Path target)
                throws Exception
        {
            Path source = Path.of(defaultDataLocation());
            Path sourceHits = source.resolve("hits");
            if (!Files.isDirectory(sourceHits)) {
                throw new IllegalStateException("Source data not found at " + sourceHits + ". Run `testing/benchmark-data/hydrate.sh hive-clickbench` first.");
            }
            if (target.toAbsolutePath().normalize().equals(source.toAbsolutePath().normalize())) {
                throw new IllegalStateException("Target must differ from source (" + source + "); pass --data");
            }
            Files.createDirectories(target);
            try (DistributedQueryRunner runner = applyDataGenerationConfiguration(HiveQueryRunner.builder())
                    .setSkipTimezoneSetup(true)
                    .addHiveProperty("hive.parquet.time-zone", "UTC")
                    .addHiveProperty("hive.metastore.disable-location-checks", "true")
                    .addHiveProperty("hive.compression-codec", "SNAPPY")
                    .build()) {
                Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());
                String sourcePath = sourceHits.toAbsolutePath().normalize().toUri().toString();
                runner.execute("CREATE SCHEMA hive.clickbench_src");
                runner.execute(format(
                        "CREATE TABLE hive.clickbench_src.hits (%s) WITH (external_location = '%s', format = 'PARQUET')",
                        HITS_COLUMNS,
                        sourcePath));

                String targetSchemaLocation = target.toAbsolutePath().normalize().toUri().toString();
                runner.execute(session, format("CREATE SCHEMA hive.clickbench WITH (location = '%s')", targetSchemaLocation));
                log.info("Generating clickbench.hits from %s -> %shits", sourcePath, targetSchemaLocation);
                runner.execute(session, "CREATE TABLE hive.clickbench.hits WITH (format = 'PARQUET') AS SELECT * FROM hive.clickbench_src.hits");
            }
            BenchmarkRunner.cleanCrcFiles(target);
        }
    }
}
