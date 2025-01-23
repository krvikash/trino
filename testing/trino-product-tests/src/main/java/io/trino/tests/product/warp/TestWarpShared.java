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
import io.trino.tempto.query.QueryExecutor;
import io.trino.tests.product.warp.utils.DemoterUtils;
import io.trino.tests.product.warp.utils.FastWarming;
import io.trino.tests.product.warp.utils.QueryUtils;
import io.trino.tests.product.warp.utils.RestUtils;
import io.trino.tests.product.warp.utils.RuleUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import io.trino.tests.product.warp.utils.TestUtils;
import io.trino.tests.product.warp.utils.WarmUtils;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_SHARED;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestWarpShared
{
    private static final Logger logger = Logger.get(TestWarpShared.class);

    private static final String CATALOG_1_NAME = "warp_1";
    private static final String CATALOG_2_NAME = "warp_2";

    @Inject
    DemoterUtils demoterUtils;
    @Inject
    QueryUtils queryUtils;
    @Inject
    RuleUtils ruleUtils;
    @Inject
    WarmUtils warmUtils;

    private final String formattedDateTime;

    public TestWarpShared()
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        formattedDateTime = now.format(formatter);
    }

    @DataProvider
    public Iterator<TestFormat> synth_shared(ITestContext context)
            throws Exception
    {
        String filePath = "file:///docker/trino-product-tests/warp/synth_shared.json";
        logger.info("running %s", filePath);
        return TestUtils.executeDataProvider(filePath);
    }

    @Test(groups = {WARP_SPEED_SHARED, PROFILE_SPECIFIC_TESTS}, dataProvider = "synth_shared")
    public void testSharedMatchCollectIds(TestFormat testFormat)
            throws IOException
    {
        executeTest(CATALOG_1_NAME, RestUtils.CATALOG_1_PORT, testFormat);
        executeTest(CATALOG_2_NAME, RestUtils.CATALOG_2_PORT, testFormat);
    }

    private void executeTest(String catalogName, int port, TestFormat testFormat)
            throws IOException
    {
        String testSchemaName = "synthetic";
        String testTableName = testFormat.name();

        try (QueryExecutor queryExecutor = onTrino()) {
            queryExecutor.executeQuery("USE %s.%s".formatted(catalogName, testSchemaName));

            // fake query to ensure that the dynamic catalog is loaded
            queryExecutor.executeQuery("select count(*) from %s".formatted(testTableName));

            queryExecutor.executeQuery("set session %s.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'"
                    .formatted(catalogName, formattedDateTime));
            Map<String, Object> sessionProperties = testFormat.session_properties() != null ?
                    testFormat.session_properties().entrySet().stream().collect(Collectors.toMap(e -> catalogName + "." + e.getKey(), Map.Entry::getValue)) :
                    Map.of();
            boolean defaultWarming = (boolean) sessionProperties.getOrDefault("%s.enable_default_warming".formatted(catalogName), false);
            if (!defaultWarming) {
                ruleUtils.createWarmupRules(port, testSchemaName, testFormat);
            }
            warmUtils.setSessions(sessionProperties);

            boolean fastWarming = (boolean) sessionProperties.getOrDefault("%s.enable_import_export".formatted(catalogName), true);
            if (fastWarming) {
                warmUtils.warmAndValidate(port, catalogName, testFormat, FastWarming.EXPORT);
                demoterUtils.demote(port, testSchemaName, testTableName, testFormat);
                demoterUtils.resetToDefaultDemoterConfiguration(port);
                warmUtils.warmAndValidate(port, catalogName, testFormat, FastWarming.IMPORT);
            }
            else {
                warmUtils.warmAndValidate(port, catalogName, testFormat, FastWarming.NONE);
            }
            warmUtils.resetSessions(sessionProperties);

            queryUtils.runQueries(catalogName, testFormat);
            logger.info("catalog[%s] successfully finish run test %s", catalogName, testFormat.name());
        }
        catch (Throwable e) {
            logger.error(e, "failed on test %s", testFormat.name());
            throw new RuntimeException(e);
        }
        finally {
            ruleUtils.resetTableRules(port, testSchemaName, testFormat);
            demoterUtils.demote(port, testSchemaName, testTableName, testFormat);
            demoterUtils.resetToDefaultDemoterConfiguration(port);
        }
    }
}
