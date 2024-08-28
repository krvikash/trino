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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.jdbc.TrinoResultSet;
import io.trino.tempto.query.QueryResult;
import org.testng.SkipException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static io.trino.tests.product.utils.QueryExecutors.onTrino;
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

    public void execute(TestFormat testFormat, boolean isWarp, String schemaName)
            throws IOException
    {
        if (testFormat.skip() || testFormat.skip_caching()) {
            logger.info("test %s is skipped. description=%s", testFormat.name(), testFormat.description());
            throw new SkipException("Skipping this test");
        }
        int ranQueries = 0;
        try {
            if (isWarp) {
                demoterUtils.resetToDefaultDemoterConfiguration(true);
            }
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
        catch (Throwable e) {
            logger.error(e, "failed on test=%s", testFormat.name());
            throw e;
        }
        finally {
            if (ranQueries > 0 && isWarp) {
                logger.info("run demoter after running %s queries on test %s", ranQueries, testFormat.name());
                demoterUtils.demoteAllByMaxUsage(true);
                demoterUtils.resetToDefaultDemoterConfiguration(true);
            }
        }
    }

    public void runQueries(List<TestFormat.QueryData> queries, boolean validateLoadFromCache)
    {
        for (TestFormat.QueryData queryData : queries) {
            logger.info("run query=%s, queryId=%s, validateLoadFromCache=%s", queryData.query(), queryData.query_id(), validateLoadFromCache);
            QueryResult queryResult = onTrino().executeQuery(queryData.query());
            List<Object> expectedResult = queryData.expected_result();
            queryUtils.verifyQueryResult(queryResult, expectedResult, queryData.query_id());
            if (validateLoadFromCache) {
                String queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
                queryUtils.validateLoadByCacheDataOperator(queryId);
            }
        }
    }
}
