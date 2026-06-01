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
 * The blob-level metastore. Stores {@link RawMaterializationDefinition} entries
 * without interpreting their IR contents. The version-aware adapter
 * ({@code VersionAwareMaterializationMetastore}) sits above this and converts to
 * and from the typed {@link MaterializationDefinition}.
 * This makes the metastore to store materialization irrespective of the IR version, allowing the IR
 * to evolve independently of the materialization metastore implementation.
 */
public interface RawMaterializationMetastore
{
    List<RawMaterializationDefinition> listMaterializations();

    void createOrReplace(RawMaterializationDefinition materializationDefinition);

    void remove(CatalogSchemaTableName materializedViewName);

    void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId);
}
