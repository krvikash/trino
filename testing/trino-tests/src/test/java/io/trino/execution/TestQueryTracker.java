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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.trino.SystemSessionProperties.QUERY_MAX_PLANNING_TIME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Tests need to finish before strict timeouts. Any background work
// may make them flaky
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // CountDownLatches are shared mutable state
public class TestQueryTracker
        extends AbstractTestQueryFramework
{
    private final CountDownLatch freeze = new CountDownLatch(1);
    private final CountDownLatch interrupted = new CountDownLatch(1);

    @AfterAll
    public void unfreeze()
    {
        freeze.countDown();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setSystemProperty(QUERY_MAX_PLANNING_TIME, "2s")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner
                .builder(defaultSession)
                .addCoordinatorProperty("query.max-splits-per-table.config-file", getResourceFilePath("max_allowed_split_count.json"))
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("col", VARCHAR)))
                        // Apply filter happens inside optimizer so this should model most blocking tasks in planning phase
                        .withApplyFilter((_, _, _) -> freeze())
                        .build());
            }
        });
        queryRunner.createCatalog("mock", "mock");
        queryRunner.createCatalog("blackhole", "blackhole", ImmutableMap.of());
        queryRunner.execute(
                """
                CREATE TABLE blackhole.default.table_within_split_limit (c BIGINT)
                WITH (split_count = 399, pages_per_split = 1, rows_per_page = 2000)
                """);
        queryRunner.execute(
                """
                CREATE TABLE blackhole.default.\"table_exceeding_split_limit.abc\" (c BIGINT)
                WITH (split_count = 500, pages_per_split = 1, rows_per_page = 2000, page_processing_delay = '1s')
                """);
        return queryRunner;
    }

    @Test
    @Timeout(10)
    public void testInterruptApplyFilter()
            throws InterruptedException
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM t1 WHERE col = 'abc'"))
                .hasMessageContaining("Query exceeded the maximum planning time limit of 2.00s");

        interrupted.await();
    }

    @Test
    @Timeout(30)
    public void testEnforceSplitCountLimitShouldFailQuery()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT count(*) FROM blackhole.default.\"table_exceeding_split_limit.abc\""))
                .hasMessageContaining("Split count 500 exceeds upper limit for table blackhole.default.\"table_exceeding_split_limit.abc\". The limit is equal to 100");
        assertThat(getQueryRunner().execute("SELECT count(*) FROM blackhole.default.table_within_split_limit").getRowCount()).isEqualTo(1);
        assertThat(getQueryRunner().execute("(SELECT * FROM blackhole.default.table_within_split_limit) UNION ALL (SELECT * FROM blackhole.default.table_within_split_limit)").getRowCount()).isEqualTo(1596000);
    }

    @Test
    public void testMaxAllowedSplitCountPerTableFileDoesNotExistOnInit()
            throws IOException
    {
        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(Files.createTempFile("prefix", "suffix").toFile().getAbsolutePath());

        assertThatThrownBy(() -> new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(new TestingTicker(), maxSplitsPerTableConfig))
                .hasMessageContaining("Could not parse JSON");
    }

    @Test
    public void testMaxAllowedSplitCountPerTableFileIsInvalidOnInit()
    {
        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(getResourceFilePath("max_allowed_split_count_invalid.json"));

        assertThatThrownBy(() -> new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(new TestingTicker(), maxSplitsPerTableConfig))
                .hasMessageContaining("Could not parse JSON");
    }

    @Test
    public void testMaxAllowedSplitCountPerTableFileIsInvalidOnRefresh()
            throws IOException
    {
        File tmpFile = Files.createTempFile("prefix", "suffix").toFile();
        Files.copy(Path.of(getResourceFilePath("max_allowed_split_count.json")), tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setRefreshPeriod(new Duration(60, TimeUnit.SECONDS));
        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(tmpFile.getAbsolutePath());

        TestingTicker ticker = new TestingTicker();
        MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider provider = new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(ticker, maxSplitsPerTableConfig);
        assertThat(provider.getMaxAllowedSplitCountPerTable()).isNotEmpty();

        Files.copy(Path.of(getResourceFilePath("max_allowed_split_count_invalid.json")), tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        ticker.increment(120, TimeUnit.SECONDS);
        assertThat(provider.getMaxAllowedSplitCountPerTable()).isNotEmpty();
    }

    @Test
    public void testMaxAllowedSplitCountPerTableFileIsRefreshed()
            throws IOException
    {
        File tmpFile = Files.createTempFile("prefix", "suffix").toFile();
        Files.copy(Path.of(getResourceFilePath("max_allowed_split_count.json")), tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setRefreshPeriod(new Duration(60, TimeUnit.SECONDS));
        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(tmpFile.getAbsolutePath());

        TestingTicker ticker = new TestingTicker();
        MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider provider = new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(ticker, maxSplitsPerTableConfig);
        assertThat(provider.getMaxAllowedSplitCountPerTable().get(QualifiedObjectName.valueOf("blackhole.default.t2"))).isEqualTo(100L);

        ticker.increment(120, TimeUnit.SECONDS);
        Files.copy(Path.of(getResourceFilePath("max_allowed_split_count_2.json")), tmpFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        assertThat(provider.getMaxAllowedSplitCountPerTable().get(QualifiedObjectName.valueOf("blackhole.default.t2"))).isEqualTo(200L);
    }

    @Test
    public void testMaxAllowedSplitCountPerTableTableOccursTwiceOnStartup()
    {
        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(getResourceFilePath("max_allowed_split_count_invalid_2.json"));
        assertThatThrownBy(() -> new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(new TestingTicker(), maxSplitsPerTableConfig))
                .hasMessageContaining("Multiple entries with same key: blackhole.default.t1=400 and blackhole.default.t1=100");
    }

    @Test
    public void testMaxAllowedSplitCountPerTableFileTableOccursTwiceOnRefresh()
    {
        MaxSplitsPerTableConfig maxSplitsPerTableConfig = new MaxSplitsPerTableConfig();
        maxSplitsPerTableConfig.setRefreshPeriod(new Duration(60, TimeUnit.SECONDS));
        TestingTicker ticker = new TestingTicker();
        MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider provider = new MaxSplitsPerTableSpec.MaxSplitsPerTableSpecProvider(ticker, maxSplitsPerTableConfig);

        maxSplitsPerTableConfig.setQueryMaxSplitsPerTableConfigFilePath(getResourceFilePath("max_allowed_split_count_invalid_2.json"));
        ticker.increment(120, TimeUnit.SECONDS);
        assertThat(provider.getMaxAllowedSplitCountPerTable()).isEmpty();
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private <T> T freeze()
    {
        try {
            freeze.await();
        }
        catch (InterruptedException e) {
            interrupted.countDown();
            throw new RuntimeException(e);
        }

        return null;
    }
}
