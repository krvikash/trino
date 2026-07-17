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
package io.trino.server.starburst.accesscontrol;

import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import io.airlift.configuration.Config;
import io.starburst.stargate.id.CatalogId;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Map.Entry;

public class GalaxyAccessControlConfig
{
    private BiMap<String, CatalogId> catalogNames = ImmutableBiMap.of();

    @NotNull
    public BiMap<String, CatalogId> getCatalogNames()
    {
        return catalogNames;
    }

    public GalaxyAccessControlConfig setCatalogNames(Map<String, CatalogId> catalogNames)
    {
        this.catalogNames = ImmutableBiMap.copyOf(catalogNames);
        return this;
    }

    @Config("galaxy.catalog-names")
    public GalaxyAccessControlConfig setCatalogNames(String catalogNames)
    {
        this.catalogNames = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("->").split(catalogNames).entrySet().stream()
                .collect(ImmutableBiMap.toImmutableBiMap(Entry::getKey, entry -> new CatalogId(entry.getValue())));
        return this;
    }
}
