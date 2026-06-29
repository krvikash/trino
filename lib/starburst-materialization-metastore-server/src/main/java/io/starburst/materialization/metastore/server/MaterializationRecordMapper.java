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

import io.airlift.json.JsonCodec;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.mapJsonCodec;

public class MaterializationRecordMapper
        implements RowMapper<RawMaterializationDefinition>
{
    static final JsonCodec<Map<String, Integer>> IR_VERSIONS_CODEC = mapJsonCodec(String.class, Integer.class);
    static final JsonCodec<Map<CatalogName, ConnectorIdVersions>> CATALOG_IR_VERSIONS_CODEC = mapJsonCodec(CatalogName.class, ConnectorIdVersions.class);

    @Override
    public RawMaterializationDefinition map(ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        CatalogSchemaTableName mvName = new CatalogSchemaTableName(
                rs.getString("source_catalog"),
                rs.getString("source_schema"),
                rs.getString("source_table"));
        StorageTableId storageTableId = new StorageTableId(
                new CatalogName(rs.getString("storage_table_catalog")),
                new ConnectorStorageTableId(
                        rs.getString("storage_table_schema"),
                        rs.getString("storage_table_name"),
                        rs.getString("storage_table_unique_id")));
        long graceMillis = rs.getLong("grace_period_millis");
        Optional<Duration> gracePeriod = rs.wasNull() ? Optional.empty() : Optional.of(Duration.ofMillis(graceMillis));
        return new RawMaterializationDefinition(
                IR_VERSIONS_CODEC.fromJson(rs.getString("ir_versions")),
                CATALOG_IR_VERSIONS_CODEC.fromJson(rs.getString("catalog_ir_versions")),
                rs.getString("computation_plan_root"),
                storageTableId,
                new MaterializedViewSource(mvName),
                rs.getObject("last_known_fresh_time", LocalDateTime.class).toInstant(ZoneOffset.UTC),
                gracePeriod);
    }
}
