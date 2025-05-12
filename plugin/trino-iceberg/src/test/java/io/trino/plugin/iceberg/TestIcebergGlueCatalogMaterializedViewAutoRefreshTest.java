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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TestIcebergGlueCatalogMaterializedViewAutoRefreshTest
        extends TestIcebergHiveCatalogMaterializedViewAutoRefreshTest
{
    @Override
    protected Map<String, String> getIcebergCatalogProperties()
    {
        return ImmutableMap.of(
                "iceberg.catalog.type", "glue",
                "hive.metastore.glue.default-warehouse-dir", "local:///glue");
    }
}
