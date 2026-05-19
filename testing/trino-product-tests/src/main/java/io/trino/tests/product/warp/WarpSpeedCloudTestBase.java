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
package io.trino.tests.product.warp;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.warp.tools.util.StringUtils;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.TestUtils;
import io.trino.tests.product.warp.utils.WarmUtils;
import io.trino.tests.product.warp.utils.syntheticconfig.TableType;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public abstract class WarpSpeedCloudTestBase
{
    private static final Logger logger = Logger.get(WarpSpeedCloudTestBase.class);

    private static final String CATALOG_NAME = "warp";
    private static final String SCHEMA_NAME = "product_tests";
    private static final String TABLE_NAME = "nation";

    @Inject
    WarmUtils warmUtils;
    @Inject
    DemoterUtils demoterUtils;
    @Inject
    QueryUtils queryUtils;

    private boolean initialized;
    private int finished;
    private String schema;
    private String table;

    protected WarpSpeedCloudTestBase() {}

    @BeforeMethodWithContext
    public void beforeMethod()
    {
        synchronized (this) {
            if (!initialized) {
                setUp();
                cleanup();

                String suffix = StringUtils.randomAlphanumeric(4);
                this.schema = SCHEMA_NAME + "_" + suffix;
                this.table = TABLE_NAME + "_" + suffix;

                initialized = true;
            }
        }
    }

    @AfterMethodWithContext
    public void afterMethod()
    {
        synchronized (this) {
            finished++;
            if (finished == countTestMethods()) {
                cleanup();
            }
        }
    }

    protected abstract void setUp();

    protected abstract int countTestMethods();

    protected abstract String getPathForSchema(String schemaName);

    // cannot use external table since the bucket in Azure and GCP has a lifecycle rule
    protected void testResiliencyBase(TestFormat testFormat)
            throws IOException
    {
        testFormat = testFormat.withTableType(table, TableType.warp);

        try (QueryExecutor queryExecutor = onTrino()) {
            queryExecutor.executeQuery("CREATE SCHEMA IF NOT EXISTS %s.%s WITH (location = '%s')"
                    .formatted(CATALOG_NAME, schema, getPathForSchema(SCHEMA_NAME)));

            queryExecutor.executeQuery("USE %s.%s".formatted(CATALOG_NAME, schema));
            queryExecutor.executeQuery("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM tpch.tiny.nation".formatted(table));

            Map<String, Object> sessionProperties = testFormat.session_properties() != null ?
                    testFormat.session_properties().entrySet().stream().collect(Collectors.toMap(e -> CATALOG_NAME + "." + e.getKey(), Map.Entry::getValue)) :
                    Map.of();
            warmUtils.setSessions(sessionProperties);

            boolean fastWarming = (boolean) sessionProperties.getOrDefault(CATALOG_NAME + ".enable_import_export", true);
            if (fastWarming) {
                warmUtils.warmAndValidate(CATALOG_NAME, testFormat, FastWarming.EXPORT);
                demoterUtils.demote(RestUtils.CATALOG_1_PORT, schema, table, testFormat);
                demoterUtils.resetToDefaultDemoterConfiguration(RestUtils.CATALOG_1_PORT);
                warmUtils.warmAndValidate(CATALOG_NAME, testFormat, FastWarming.IMPORT);
            }
            else {
                warmUtils.warmAndValidate(CATALOG_NAME, testFormat, FastWarming.NONE);
            }
            warmUtils.resetSessions(sessionProperties);

            queryUtils.runQueries(CATALOG_NAME, testFormat);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Throwable e) {
            logger.error(e, "failed on test %s", testFormat.name());
            throw new RuntimeException(e);
        }
        finally {
            demoterUtils.demote(RestUtils.CATALOG_1_PORT, schema, table, testFormat);
            demoterUtils.resetToDefaultDemoterConfiguration(RestUtils.CATALOG_1_PORT);
            cleanup();
        }
    }

    @DataProvider
    public Iterator<TestFormat> synth_clouds(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/trino-product-tests/warp/synth_clouds.json";
        logger.info("running %s", filePath);
        return TestUtils.executeDataProvider(filePath);
    }

    private void cleanup()
    {
        try (QueryExecutor queryExecutor = onTrino()) {
            if (StringUtils.isNotEmpty(schema)) {
                if (StringUtils.isNotEmpty(table)) {
                    queryExecutor.executeQuery("DROP TABLE IF EXISTS %s.%s.%s".formatted(CATALOG_NAME, schema, table));
                }
                queryExecutor.executeQuery("DROP SCHEMA IF EXISTS %s.%s".formatted(CATALOG_NAME, schema));
            }
        }
        catch (Throwable e) {
            logger.error(e, "failed cleaning up table/schema");
        }
    }
}
