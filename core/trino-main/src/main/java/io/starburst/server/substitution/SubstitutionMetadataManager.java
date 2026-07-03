/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SubstitutionMetadataManager
        implements SubstitutionMetadata
{
    private final CatalogServiceProvider<Optional<ConnectorSubstitutionMetadata>> substitutionMetadataProvider;

    @Inject
    public SubstitutionMetadataManager(CatalogServiceProvider<Optional<ConnectorSubstitutionMetadata>> substitutionMetadataProvider)
    {
        this.substitutionMetadataProvider = requireNonNull(substitutionMetadataProvider, "substitutionMetadataProvider is null");
    }

    @Override
    public Optional<TableId> getTableId(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        return substitutionMetadataProvider.getService(catalogHandle)
                .flatMap(metadata -> metadata.getTableId(session.toConnectorSession(catalogHandle), tableHandle.connectorHandle()))
                .map(connectorTableId -> new TableId(catalogHandle.getCatalogName(), connectorTableId));
    }

    @Override
    public Optional<StorageTableId> getStorageTableId(Session session, TableHandle tableHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        return substitutionMetadataProvider.getService(catalogHandle)
                .flatMap(substitutionMetadata -> substitutionMetadata.getStorageTableId(
                                session.toConnectorSession(catalogHandle),
                                tableHandle.connectorHandle())
                        .map(connectorStorageTableId -> new StorageTableId(catalogHandle.getCatalogName(), connectorStorageTableId)));
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        return getRequiredCatalogMetadata(catalogHandle)
                .getColumnId(session.toConnectorSession(catalogHandle), columnHandle);
    }

    @Override
    public boolean tableHandleMatchesId(Session session, TableHandle queryTable, TableId candidateTable)
    {
        CatalogHandle catalogHandle = queryTable.catalogHandle();
        if (!catalogHandle.getCatalogName().equals(candidateTable.catalogName())) {
            return false;
        }
        return substitutionMetadataProvider.getService(catalogHandle)
                .map(metadata -> metadata.tableHandleMatchesId(
                        session.toConnectorSession(catalogHandle),
                        queryTable.connectorHandle(),
                        candidateTable.connectorId()))
                .orElse(false);
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions(CatalogHandle catalogHandle)
    {
        return substitutionMetadataProvider.getService(catalogHandle)
                .map(ConnectorSubstitutionMetadata::tableIdVersions)
                .orElseGet(ImmutableSet::of);
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions(CatalogHandle catalogHandle)
    {
        return substitutionMetadataProvider.getService(catalogHandle)
                .map(ConnectorSubstitutionMetadata::columnIdVersions)
                .orElseGet(ImmutableSet::of);
    }

    private ConnectorSubstitutionMetadata getRequiredCatalogMetadata(CatalogHandle catalogHandle)
    {
        return substitutionMetadataProvider.getService(catalogHandle)
                .orElseThrow(() -> new IllegalArgumentException("Catalog '%s', does not support materialized view substitution".formatted(catalogHandle)));
    }
}
