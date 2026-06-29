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

import com.google.inject.Inject;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.connector.CatalogSchemaTableName;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.materialization.metastore.server.MaterializationRecordMapper.CATALOG_IR_VERSIONS_CODEC;
import static io.starburst.materialization.metastore.server.MaterializationRecordMapper.IR_VERSIONS_CODEC;
import static java.util.Objects.requireNonNull;

public class DbRawMaterializationMetastore
{
    private final MaterializationDao dao;

    @Inject
    public DbRawMaterializationMetastore(MaterializationDao dao)
    {
        this.dao = requireNonNull(dao, "dao is null");
    }

    public List<RawMaterializationDefinition> listMaterializations(MetastoreId metastoreId)
    {
        return dao.list(metastoreId.id());
    }

    public void createOrReplace(MetastoreId metastoreId, RawMaterializationDefinition definition)
    {
        Instant now = Instant.now();
        dao.createOrReplace(toRow(metastoreId.id(), definition, now, now));
    }

    public void remove(MetastoreId metastoreId, CatalogSchemaTableName name)
    {
        dao.deleteBySourceName(metastoreId.id(), name.getCatalogName(), name.getSchemaTableName().getSchemaName(), name.getSchemaTableName().getTableName());
    }

    public void renameIfExists(MetastoreId metastoreId, CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        dao.rename(
                metastoreId.id(),
                source.getCatalogName(),
                source.getSchemaTableName().getSchemaName(),
                source.getSchemaTableName().getTableName(),
                target.getCatalogName(),
                target.getSchemaTableName().getSchemaName(),
                target.getSchemaTableName().getTableName(),
                targetStorageTableId.catalogName().toString(),
                targetStorageTableId.connectorStorageTableId().schemaName(),
                targetStorageTableId.connectorStorageTableId().tableName(),
                targetStorageTableId.connectorStorageTableId().uniqueId(),
                LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC));
    }

    private static MaterializationRow toRow(String metastoreId, RawMaterializationDefinition definition, Instant createdAt, Instant lastModifiedAt)
    {
        checkArgument(definition.source() instanceof MaterializedViewSource, "source is not a materialized view %s", definition.source());
        CatalogSchemaTableName source = ((MaterializedViewSource) definition.source()).materializedViewName();
        StorageTableId storage = definition.storageTableId();
        return new MaterializationRow(
                metastoreId,
                source.getCatalogName(),
                source.getSchemaTableName().getSchemaName(),
                source.getSchemaTableName().getTableName(),
                storage.catalogName().toString(),
                storage.connectorStorageTableId().schemaName(),
                storage.connectorStorageTableId().tableName(),
                storage.connectorStorageTableId().uniqueId(),
                LocalDateTime.ofInstant(definition.lastKnownFreshTime(), ZoneOffset.UTC),
                definition.gracePeriod().map(Duration::toMillis).orElse(null),
                IR_VERSIONS_CODEC.toJson(definition.irVersions()),
                CATALOG_IR_VERSIONS_CODEC.toJson(definition.catalogIrVersions()),
                definition.computationPlanRootJson(),
                LocalDateTime.ofInstant(createdAt, ZoneOffset.UTC),
                LocalDateTime.ofInstant(lastModifiedAt, ZoneOffset.UTC));
    }

    public record MaterializationRow(
            String metastoreId,
            String sourceCatalogName,
            String sourceSchemaName,
            String sourceTableName,
            String storageCatalogName,
            String storageSchemaName,
            String storageTableName,
            String storageUniqueId,
            LocalDateTime lastKnownFreshTime,
            @Nullable Long gracePeriodMillis,
            String irVersions,
            String catalogIrVersions,
            String computationPlanRoot,
            LocalDateTime createdAt,
            LocalDateTime lastModifiedAt)
    {
        public MaterializationRow
        {
            requireNonNull(metastoreId, "metastoreId is null");
            requireNonNull(sourceCatalogName, "sourceCatalogName is null");
            requireNonNull(sourceSchemaName, "sourceSchemaName is null");
            requireNonNull(sourceTableName, "sourceTableName is null");
            requireNonNull(storageCatalogName, "storageCatalogName is null");
            requireNonNull(storageSchemaName, "storageSchemaName is null");
            requireNonNull(storageTableName, "storageTableName is null");
            requireNonNull(storageUniqueId, "storageUniqueId is null");
            requireNonNull(lastKnownFreshTime, "lastKnownFreshTime is null");
            requireNonNull(irVersions, "irVersions is null");
            requireNonNull(catalogIrVersions, "catalogIrVersions is null");
            requireNonNull(computationPlanRoot, "computationPlanRoot is null");
            requireNonNull(createdAt, "createdAt is null");
            requireNonNull(lastModifiedAt, "lastModifiedAt is null");
        }
    }
}
