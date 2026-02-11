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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestIcebergGlueCatalogConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergGlueCatalogConfig.class)
                .setCacheTableMetadata(true)
                .setMetastoreCacheTtl(new Duration(0, SECONDS))
                .setMetastoreCacheRefreshInterval(null)
                .setMetastoreCacheMaximumSize(20_000)
                .setMetastoreCacheMaxRefreshThreads(10));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.glue.cache-table-metadata", "false")
                .put("iceberg.glue.metastore-cache.ttl", "5m")
                .put("iceberg.glue.metastore-cache.refresh-interval", "1m")
                .put("iceberg.glue.metastore-cache.maximum-size", "1000")
                .put("iceberg.glue.metastore-cache.max-refresh-threads", "5")
                .buildOrThrow();

        IcebergGlueCatalogConfig expected = new IcebergGlueCatalogConfig()
                .setCacheTableMetadata(false)
                .setMetastoreCacheTtl(new Duration(5, MINUTES))
                .setMetastoreCacheRefreshInterval(new Duration(1, MINUTES))
                .setMetastoreCacheMaximumSize(1000)
                .setMetastoreCacheMaxRefreshThreads(5);

        assertFullMapping(properties, expected);
    }
}
