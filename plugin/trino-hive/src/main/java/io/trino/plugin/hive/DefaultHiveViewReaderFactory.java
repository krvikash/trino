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
package io.trino.plugin.hive;

import com.google.inject.Inject;
import io.trino.metastore.Table;
import io.trino.plugin.hive.ViewReaderUtil.ViewReader;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class DefaultHiveViewReaderFactory
        implements HiveViewReaderFactory
{
    private final TypeManager typeManager;
    private final MetadataProvider metadataProvider;
    private final boolean runHiveViewRunAsInvoker;
    private final HiveTimestampPrecision hiveTimestampPrecision;
    private final SchemaMappingPrefixes schemaMappingPrefixes;

    @Inject
    public DefaultHiveViewReaderFactory(
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            HiveConfig hiveConfig,
            SchemaMappingPrefixes schemaMappingPrefixes)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.runHiveViewRunAsInvoker = hiveConfig.isHiveViewsRunAsInvoker();
        this.hiveTimestampPrecision = hiveConfig.getTimestampPrecision();
        this.schemaMappingPrefixes = requireNonNull(schemaMappingPrefixes, "schemaMappingPrefixes is null");
    }

    @Override
    public ViewReader createViewReader(
            ConnectorSession session,
            SemiTransactionalHiveMetastore metastore,
            SchemaTableName viewName,
            Table table,
            BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver)
    {
        return ViewReaderUtil.createViewReader(
                metastore,
                session,
                viewName,
                table,
                typeManager,
                tableRedirectionResolver,
                metadataProvider,
                runHiveViewRunAsInvoker,
                hiveTimestampPrecision,
                schemaMappingPrefixes);
    }
}
