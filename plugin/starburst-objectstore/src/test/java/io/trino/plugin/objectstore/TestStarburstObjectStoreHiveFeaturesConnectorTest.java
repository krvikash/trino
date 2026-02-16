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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;

import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;

public class TestStarburstObjectStoreHiveFeaturesConnectorTest
        extends BaseTestObjectStoreHiveFeaturesConnectorTest
{
    public TestStarburstObjectStoreHiveFeaturesConnectorTest()
    {
        super(STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                // Hive setting synced from BaseHiveConnectorTest
                .put("hive.allow-register-partition-procedure", "true")
                // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                .put("hive.writer-sort-buffer-size", "1MB")
                // Make weighted split scheduling more conservative to avoid OOMs in test
                .put("hive.minimum-assigned-split-weight", "0.5")
                // Hive setting synced from HiveQueryRunner
                .put("hive.max-partitions-per-scan", "1000")
                .put("hive.max-partitions-for-eager-load", "1000")
                // ObjectStore
                .put("great-lakes.table-type", TableType.HIVE.name())
                .buildOrThrow());
    }
}
