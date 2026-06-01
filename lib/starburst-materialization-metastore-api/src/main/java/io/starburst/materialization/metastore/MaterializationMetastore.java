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

import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.List;

/**
 * Typed metastore for materialization definitions. Sits above {@link RawMaterializationMetastore}
 * and operates on fully deserialized {@link MaterializationDefinition} objects.
 */
public interface MaterializationMetastore
{
    /**
     * Returns all known materialization definitions.
     */
    List<MaterializationDefinition> listMaterializations();

    /**
     * Creates or replaces the materialization definition for the given MV.
     */
    void createOrReplace(MaterializationDefinition materializationDefinition);

    /**
     * Removes the materialization definition for the given MV, if present.
     */
    void remove(CatalogSchemaTableName materializedViewName);

    /**
     * Renames the materialization entry when the underlying MV is renamed.
     */
    void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId);
}
