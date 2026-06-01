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

import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.Optional;
import java.util.Set;

public interface SubstitutionMetadata
{
    Optional<TableId> getTableId(Session session, TableHandle tableHandle);

    /**
     * Extracts the storage table identity captured at MV upsert time. Returns empty when the
     * connector cannot produce one for this handle (e.g. the storage is not tagged because
     * the MV pre-dates the substitution feature).
     */
    Optional<StorageTableId> getStorageTableId(Session session, TableHandle tableHandle);

    /**
     * Returns the column identity for the given column handle, or empty when the column
     * handle does not have a stable identity (for example a computed expression or
     * synthetic derivation that is not a column of the underlying table).
     * <p>
     * Precondition: the catalog must support substitution metadata, which is implied by
     * a prior call to {@link #tableHandleMatchesId} returning true or {@link #getTableId}
     * returning a non-empty value for a handle in the same catalog. Calling this method
     * for a catalog without substitution support is a programming error.
     *
     * @throws IllegalArgumentException if the catalog does not provide substitution metadata
     */
    Optional<ConnectorColumnId> getColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle);

    /**
     * Engine-side wrapper for {@link io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata#tableHandleMatchesId}.
     */
    boolean tableHandleMatchesId(Session session, TableHandle queryTable, TableId candidateTable);

    /**
     * Returns the format version of {@link io.trino.spi.connector.substitution.ConnectorTableId}
     * produced by the given catalog. Used to detect stored materializations whose table identity
     * format is incompatible with the current engine/connector.
     */
    Set<ConnectorIdVersion> tableIdVersions(CatalogHandle catalogHandle);

    /**
     * Returns the format version of {@link ConnectorColumnId} produced by the given
     * catalog. Used to detect stored materializations whose column identity format is
     * incompatible with the current engine/connector.
     */
    Set<ConnectorIdVersion> columnIdVersions(CatalogHandle catalogHandle);
}
