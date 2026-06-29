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
package io.starburst.materialization.metastore.server.db;

import io.starburst.materialization.metastore.server.DbRawMaterializationMetastore.MaterializationRow;
import io.starburst.materialization.metastore.server.MaterializationDao;
import org.jdbi.v3.sqlobject.customizer.BindMethods;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface MySqlMaterializationDao
        extends MaterializationDao
{
    @SqlUpdate(
            """
            INSERT INTO materializations (
                metastore_id,
                source_catalog, source_schema, source_table,
                storage_table_catalog, storage_table_schema, storage_table_name, storage_table_unique_id,
                last_known_fresh_time, grace_period_millis,
                ir_versions, catalog_ir_versions, computation_plan_root,
                created_at, last_modified_at)
            VALUES (
                :metastoreId,
                :sourceCatalogName, :sourceSchemaName, :sourceTableName,
                :storageCatalogName, :storageSchemaName, :storageTableName, :storageUniqueId,
                :lastKnownFreshTime, :gracePeriodMillis,
                :irVersions, :catalogIrVersions, :computationPlanRoot,
                :createdAt, :lastModifiedAt)
            ON DUPLICATE KEY UPDATE
                storage_table_catalog = :storageCatalogName,
                storage_table_schema = :storageSchemaName,
                storage_table_name = :storageTableName,
                storage_table_unique_id = :storageUniqueId,
                last_known_fresh_time = :lastKnownFreshTime,
                grace_period_millis = :gracePeriodMillis,
                ir_versions = :irVersions,
                catalog_ir_versions = :catalogIrVersions,
                computation_plan_root = :computationPlanRoot,
                last_modified_at = :lastModifiedAt
            """)
    @Override
    void createOrReplace(@BindMethods MaterializationRow row);
}
