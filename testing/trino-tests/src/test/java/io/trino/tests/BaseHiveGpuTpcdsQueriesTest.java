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
package io.trino.tests;

import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.benchmark.Tpcds;
import io.trino.tpcds.Table;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.tests.GpuQueriesTests.assertGpuQueryResultsAndOperators;
import static io.trino.tests.GpuQueriesTests.deterministicLoadSession;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public abstract class BaseHiveGpuTpcdsQueriesTest
        extends GpuQueriesTests.GpuPlanTest
{
    private static final Logger log = Logger.get(BaseHiveGpuTpcdsQueriesTest.class);

    static final int SCALE_FACTOR = 1;

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                .setCreateTpchSchemas(false)
                .setTpcdsCatalogEnabled(true)
                .setSkipTimezoneSetup(true)
                .addHiveProperty("hive.storage-format", "PARQUET")
                .addHiveProperty("hive.parquet.time-zone", "UTC");
        configureRunner(builder);
        QueryRunner runner = builder.build();
        try {
            Session loadSession = deterministicLoadSession(runner.getDefaultSession());
            runner.execute("CREATE SCHEMA hive.tpcds");
            for (Table table : Table.getBaseTables()) {
                if (table == Table.DBGEN_VERSION) {
                    continue;
                }
                String name = table.getName().toLowerCase(ENGLISH);
                long start = System.nanoTime();
                runner.execute(loadSession, "CREATE TABLE hive.tpcds.%s WITH (format = 'PARQUET') AS SELECT * FROM tpcds.sf%d.%s"
                        .formatted(name, SCALE_FACTOR, name));
                log.info("Loaded hive.tpcds.%s from tpcds.sf%d in %d ms", name, SCALE_FACTOR, (System.nanoTime() - start) / 1_000_000);
            }
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    protected abstract void configureRunner(HiveQueryRunner.Builder<?> builder);

    @ParameterizedTest(name = "{0}", quoteTextArguments = false)
    @MethodSource("queries")
    public final void testQuery(String query)
            throws Exception
    {
        String sql = readQuery(query);
        assertGpuQueryResultsAndOperators(
                getQueryRunner(),
                sql,
                readExpectedGpuPlanCoverage(query));
    }

    @Override
    final Stream<String> queries()
    {
        return Tpcds.allQueries().stream();
    }

    @Override
    final String readQuery(String query)
    {
        return Tpcds.readQuery(query, "hive", "tpcds");
    }

    private String readExpectedGpuPlanCoverage(String query)
            throws IOException
    {
        return Resources.toString(getResource(gpuPlanResource(query)), UTF_8);
    }
}
