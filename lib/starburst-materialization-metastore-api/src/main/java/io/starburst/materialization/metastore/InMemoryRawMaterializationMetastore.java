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
package io.starburst.materialization.metastore;

import com.google.common.collect.ImmutableList;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InMemoryRawMaterializationMetastore
        implements RawMaterializationMetastore
{
    private final Map<CatalogSchemaTableName, RawMaterializationDefinition> materializations = new ConcurrentHashMap<>();

    @Override
    public List<RawMaterializationDefinition> listMaterializations()
    {
        return ImmutableList.copyOf(materializations.values());
    }

    @Override
    public void createOrReplace(RawMaterializationDefinition definition)
    {
        requireNonNull(definition, "definition is null");
        checkArgument(definition.source() instanceof MaterializedViewSource, "source is not a materialized view");
        materializations.put(((MaterializedViewSource) definition.source()).materializedViewName(), definition);
    }

    @Override
    public void remove(CatalogSchemaTableName materializedViewName)
    {
        materializations.remove(requireNonNull(materializedViewName, "materializedViewName is null"));
    }

    @Override
    public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        RawMaterializationDefinition materializationDefinition = materializations.remove(source);
        if (materializationDefinition != null) {
            materializations.put(target, materializationDefinition.renamedTo(target, targetStorageTableId));
        }
    }
}
