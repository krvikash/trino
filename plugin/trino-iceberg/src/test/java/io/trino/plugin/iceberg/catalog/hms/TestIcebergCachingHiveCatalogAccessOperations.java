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
package io.trino.plugin.iceberg.catalog.hms;

import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;

final class TestIcebergCachingHiveCatalogAccessOperations
        extends BaseIcebergCachingHiveCatalogAccessOperationsTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .addIcebergProperty("hive.metastore-cache-ttl", "30m")
                .build();
    }
}
