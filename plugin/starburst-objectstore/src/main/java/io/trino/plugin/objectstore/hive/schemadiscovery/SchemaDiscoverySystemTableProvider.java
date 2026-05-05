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
package io.trino.plugin.objectstore.hive.schemadiscovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.starburst.schema.discovery.SchemaDiscoveryController;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.IdentifierConstraint;
import io.starburst.schema.discovery.trino.system.table.DiscoveryLocationAccessControlAdapter;
import io.starburst.schema.discovery.trino.system.table.SchemaDiscoverySystemTable;
import io.starburst.schema.discovery.trino.system.table.ShallowDiscoverySystemTable;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.connector.SystemTableProvider;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SchemaDiscoverySystemTableProvider
        implements SystemTableProvider
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final ExecutorService executor;
    private final ObjectMapper objectMapper;
    private final DiscoveryLocationAccessControlAdapter discoveryLocationAccessControlAdapter;
    private final IdentifierConstraint schemaDiscoveryIdentifierConstraint;

    @Inject
    public SchemaDiscoverySystemTableProvider(
            TrinoFileSystemFactory trinoFileSystemFactory,
            @ForSchemaDiscovery ExecutorService executor,
            ObjectMapper objectMapper,
            LocationAccessControl locationAccessControl,
            IdentifierConstraint schemaDiscoveryIdentifierConstraint)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.discoveryLocationAccessControlAdapter = (identity, location, _) -> locationAccessControl.checkCanUseLocation(identity, location);
        this.schemaDiscoveryIdentifierConstraint = requireNonNull(schemaDiscoveryIdentifierConstraint, "schemaDiscoveryIdentifierConstraint is null");
    }

    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName table)
    {
        return Optional.empty();
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.equals(SchemaDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            SchemaDiscoveryController controller = createSchemaDiscoveryController(session);
            SchemaDiscoverySystemTable systemTable = new SchemaDiscoverySystemTable(controller, objectMapper, discoveryLocationAccessControlAdapter);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        if (tableName.equals(ShallowDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            SchemaDiscoveryController controller = createSchemaDiscoveryController(session);
            ShallowDiscoverySystemTable systemTable = new ShallowDiscoverySystemTable(controller, objectMapper, discoveryLocationAccessControlAdapter);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        return Optional.empty();
    }

    private SchemaDiscoveryController createSchemaDiscoveryController(ConnectorSession session)
    {
        Function<URI, DiscoveryTrinoFileSystem> fileSystemProvider = _ -> new DiscoveryTrinoFileSystem(trinoFileSystemFactory.create(session));
        OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputFile) -> new HdfsOrcDataSource(id, size, options, inputFile, new FileFormatDataSourceStats());
        ParquetDataSourceFactory parquetDataSourceFactory = (inputFile) -> new TrinoParquetDataSource(inputFile, ParquetReaderOptions.defaultOptions(), new FileFormatDataSourceStats());
        return new SchemaDiscoveryController(fileSystemProvider, parquetDataSourceFactory, orcDataSourceFactory, Dialect.GALAXY, schemaDiscoveryIdentifierConstraint, executor);
    }
}
