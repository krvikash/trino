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
package io.starburst.materialization.metastore.server;

import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.server.DbRawMaterializationMetastore.MaterializationRow;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.time.LocalDateTime;
import java.util.List;

public interface MaterializationDao
        extends SqlObject
{
    void createOrReplace(MaterializationRow row);

    @SqlUpdate("DELETE FROM materializations WHERE metastore_id = :metastoreId AND source_catalog = :catalog AND source_schema = :schema AND source_table = :table")
    void deleteBySourceName(String metastoreId, String catalog, String schema, String table);

    @SqlQuery(
            """
            SELECT source_catalog, source_schema, source_table,
                   storage_table_catalog, storage_table_schema, storage_table_name, storage_table_unique_id,
                   last_known_fresh_time, grace_period_millis,
                   ir_versions, catalog_ir_versions, computation_plan_root
            FROM materializations
            WHERE metastore_id = :metastoreId
            """)
    @UseRowMapper(MaterializationRecordMapper.class)
    List<RawMaterializationDefinition> list(String metastoreId);

    @SqlUpdate(
            """
            UPDATE materializations
            SET
                source_catalog = :targetCatalog,
                source_schema = :targetSchema,
                source_table = :targetTable,
                storage_table_catalog = :targetStorageTableCatalog,
                storage_table_schema = :targetStorageTableSchema,
                storage_table_name = :targetStorageTableName,
                storage_table_unique_id = :targetStorageTableUniqueId,
                last_modified_at = :now
            WHERE
                metastore_id = :metastoreId AND
                source_catalog = :sourceCatalog AND
                source_schema = :sourceSchema AND
                source_table = :sourceTable
            """)
    void rename(
            String metastoreId,
            String sourceCatalog,
            String sourceSchema,
            String sourceTable,
            String targetCatalog,
            String targetSchema,
            String targetTable,
            String targetStorageTableCatalog,
            String targetStorageTableSchema,
            String targetStorageTableName,
            String targetStorageTableUniqueId,
            LocalDateTime now);
}
