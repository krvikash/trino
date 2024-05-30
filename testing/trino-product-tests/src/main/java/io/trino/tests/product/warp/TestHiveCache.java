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
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tests.product.warp.utils.CacheUtils;
import io.trino.tests.product.warp.utils.TestFormat;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;

import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.WARP_SPEED_HIVE_CACHE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestHiveCache
{
    private static final String CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "synthetic";

    private boolean initialized;

    @Inject
    CacheUtils cacheUtils;

    public TestHiveCache()
    {
    }

    @BeforeMethodWithContext
    public void before()
            throws Exception
    {
        synchronized (this) {
            if (!initialized) {
                onTrino().executeQuery(format("CREATE SCHEMA IF NOT EXISTS %s.%s", CATALOG_NAME, SCHEMA_NAME));
                onTrino().executeQuery(format("USE %s.%s", CATALOG_NAME, SCHEMA_NAME));
                initialized = true;
            }
        }
    }

    @AfterMethodWithContext
    public void after()
    {
    }

    @DataProvider
    public Iterator<Object[]> cache(ITestContext context)
            throws Exception
    {
        return CacheUtils.executeDataProvider("file:///docker/presto-product-tests/warp/cache.json");
    }

    @Test(groups = {WARP_SPEED_HIVE_CACHE, PROFILE_SPECIFIC_TESTS}, dataProvider = "cache")
    public void cache(TestFormat testFormat)
            throws IOException
    {
        cacheUtils.execute(testFormat, false, SCHEMA_NAME);
    }
}
