
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
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestStarburstObjectStoreMetadataListing
        extends BaseTestObjectStoreMetadataListing
{
    public TestStarburstObjectStoreMetadataListing()
    {
        super(STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                .put("hive.hive-views.enabled", "true")
                .put("great-lakes.table-type", TableType.HIVE.name())
                .buildOrThrow());
    }
}
