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
package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.testng.SkipException;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static java.lang.String.format;

public class CacheUtils
{
    private static final Logger logger = Logger.get(CacheUtils.class);

    private final String formattedDateTime;

    @Inject
    QueryUtils queryUtils;
    @Inject
    DemoterUtils demoterUtils;

    public CacheUtils()
    {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd_HHmmss");
        formattedDateTime = now.format(formatter);
    }

    public static Iterator<Object[]> executeDataProvider(String filePath)
            throws Exception
    {
        logger.info("running %s", filePath);
        JsonNode jsonNodeTests = objectMapper.readTree(new URI(filePath).toURL());
        List<TestFormat> tests = objectMapper.readerFor(new TypeReference<List<TestFormat>>() {})
                .readValue(jsonNodeTests);
        return tests.stream()
                .filter(TestFormat::pt_enable)
                .map(x -> new Object[] {x})
                .iterator();
    }

    public void execute(TestFormat testFormat, boolean isWarp, String schemaName)
            throws IOException
    {
        if (testFormat.skip() || testFormat.skip_caching()) {
            logger.info("test %s is skipped. description=%s", testFormat.name(), testFormat.description());
            throw new SkipException("Skipping this test");
        }
        demoterUtils.resetToDefaultDemoterConfiguration(true);
        int ranQueries = 0;
        try {
            logger.info("starting run test %s", testFormat.name());
            if (isWarp) {
                onTrino().executeQuery("set session warp.enable_default_warming = false");
                onTrino().executeQuery(format("set session warp.import_export_s3_path = 's3://systemtest-export-import/test_export_import/pt/%s'", formattedDateTime));
                onTrino().executeQuery("set session warp.enable_import_export = true");
                onTrino().executeQuery(format("USE warp.%s", schemaName));
            }
            ranQueries = queryUtils.runCacheQueries(testFormat, isWarp);
            logger.info("successfully finish run test %s", testFormat.name());
        }
        catch (Exception e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw e;
        }
        finally {
            if (ranQueries > 0) {
                logger.info("run demoter after running %s queries on test %s", ranQueries, testFormat.name());
                demoterUtils.demoteAllByMaxUsage();
                demoterUtils.resetToDefaultDemoterConfiguration(true);
            }
        }
    }
}
