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
package io.starburst.stargate.icehouse.catalog.rest;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.exception.RetryableIcehouseCatalogException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.starburst.stargate.icehouse.exception.TerminalIcehouseCatalogException;
import io.starburst.stargate.icehouse.spi.maintenance.IcebergTableKey;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Iceberg REST catalog implementation of {@link IcehouseCatalog}.
 */
public final class RestIcehouseCatalog
        implements IcehouseCatalog
{
    private static final Logger log = Logger.get(RestIcehouseCatalog.class);

    private final RESTSessionCatalog restCatalog;
    private final SessionContext sessionContext;

    public RestIcehouseCatalog(RESTSessionCatalog restCatalog)
    {
        this.restCatalog = requireNonNull(restCatalog, "restCatalog is null");
        this.sessionContext = SessionContext.createEmpty();
    }

    @Override
    public List<String> listSchemas()
    {
        try {
            ImmutableList.Builder<String> schemas = ImmutableList.builder();
            for (Namespace namespace : restCatalog.listNamespaces(sessionContext)) {
                schemas.add(namespace.toString());
            }
            return schemas.build();
        }
        catch (RuntimeException e) {
            throw classify("listSchemas", e);
        }
    }

    @Override
    public List<String> listTables(String schema)
    {
        try {
            ImmutableList.Builder<String> tables = ImmutableList.builder();
            for (TableIdentifier tableIdentifier : restCatalog.listTables(sessionContext, Namespace.of(schema))) {
                tables.add(tableIdentifier.name());
            }
            return tables.build();
        }
        catch (RuntimeException e) {
            throw classify("listTables(schema=%s)".formatted(schema), e);
        }
    }

    @Override
    public Table loadTable(io.starburst.stargate.icehouse.spi.TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        try {
            return restCatalog.loadTable(sessionContext, TableIdentifier.of(key.schemaName(), key.tableName()));
        }
        catch (RuntimeException e) {
            throw classify("loadTable(%s.%s)".formatted(key.schemaName(), key.tableName()), e);
        }
    }

    @Override
    public String metadataLocation(io.starburst.stargate.icehouse.spi.TableIdentifier tableId)
    {
        IcebergTableKey key = IcebergTableKey.specialize(tableId);
        try {
            BaseTable baseTable = (BaseTable) restCatalog.loadTable(sessionContext, TableIdentifier.of(key.schemaName(), key.tableName()));
            return baseTable.operations().current().metadataFileLocation();
        }
        catch (NoSuchTableException | NoSuchNamespaceException e) {
            throw new TableNotFoundException(key.schemaName(), key.tableName());
        }
        catch (RuntimeException e) {
            throw classify("metadataLocation(%s.%s)".formatted(key.schemaName(), key.tableName()), e);
        }
    }

    @Override
    public void close()
    {
        try {
            restCatalog.close();
        }
        catch (IOException | RuntimeException e) {
            log.warn(e, "Failed to close REST catalog");
        }
    }

    private static RuntimeException classify(String operation, RuntimeException e)
    {
        if (e instanceof NotAuthorizedException || e instanceof ForbiddenException) {
            return new TerminalIcehouseCatalogException(
                    "Non retryable error accessing REST catalog during %s: %s".formatted(operation, e.getMessage()), e);
        }
        return new RetryableIcehouseCatalogException(
                "Transient error accessing REST catalog during %s: %s".formatted(operation, e.getMessage()), e);
    }
}
