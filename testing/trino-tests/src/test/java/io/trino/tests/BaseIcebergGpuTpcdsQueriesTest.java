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
import io.trino.plugin.iceberg.IcebergQueryRunner;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public abstract class BaseIcebergGpuTpcdsQueriesTest
        extends GpuQueriesTests.GpuPlanTest
{
    private static final Logger log = Logger.get(BaseIcebergGpuTpcdsQueriesTest.class);

    static final int SCALE_FACTOR = 1;

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .disableSchemaInitializer()
                .setTpcdsCatalogEnabled(true)
                .addIcebergProperty("iceberg.file-format", "PARQUET");
        configureRunner(builder);
        QueryRunner runner = builder.build();
        try {
            runner.execute("CREATE SCHEMA iceberg.tpcds");
            for (Table table : Table.getBaseTables()) {
                if (table == Table.DBGEN_VERSION) {
                    continue;
                }
                String name = table.getName().toLowerCase(ENGLISH);
                long start = System.nanoTime();
                runner.execute("CREATE TABLE iceberg.tpcds.%s AS SELECT * FROM tpcds.sf%d.%s"
                        .formatted(name, SCALE_FACTOR, name));
                log.info("Loaded iceberg.tpcds.%s from tpcds.sf%d in %d ms", name, SCALE_FACTOR, (System.nanoTime() - start) / 1_000_000);
            }
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, runner);
            throw e;
        }
        return runner;
    }

    protected abstract void configureRunner(IcebergQueryRunner.Builder builder);

    @ParameterizedTest(name = "{0}", quoteTextArguments = false)
    @MethodSource("queries")
    public final void testQuery(String query)
            throws Exception
    {
        assertGpuQueryResultsAndOperators(
                getQueryRunner(),
                readQuery(query),
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
        return Tpcds.readQuery(query, "iceberg", "tpcds");
    }

    private String readExpectedGpuPlanCoverage(String query)
            throws IOException
    {
        return Resources.toString(getResource(gpuPlanResource(query)), UTF_8);
    }
}
