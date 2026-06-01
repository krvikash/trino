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

import com.google.inject.Inject;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.Session;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.sql.tree.Query;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.MaterializedViewPropertyManager.SUBSTITUTION_ENABLED;
import static io.trino.metadata.MaterializedViewPropertyManager.isSubstitutionEnabled;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static java.util.Objects.requireNonNull;

public class DefaultMaterializationService
        implements MaterializationService
{
    private final Metadata metadata;
    private final QueryPreparer queryPreparer;
    private final MaterializationIrExtractor materializationIrExtractor;
    private final SubstitutionMetadata substitutionMetadata;
    private final MaterializationMetastore materializationMetastore;

    @Inject
    public DefaultMaterializationService(
            Metadata metadata,
            QueryPreparer queryPreparer,
            MaterializationIrExtractor materializationIrExtractor,
            SubstitutionMetadata substitutionMetadata,
            MaterializationMetastore materializationMetastore)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.materializationIrExtractor = requireNonNull(materializationIrExtractor, "materializationIRExtractor is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.materializationMetastore = requireNonNull(materializationMetastore, "materializationMetastore is null");
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        // Reflect a substitution_enabled flip in the materialization metastore right away;
        // otherwise the flip is only observed at the next REFRESH, which may not run if the
        // MV is already fresh. This phase runs after the connector-level DDL has already
        // committed, so a concurrent DROP can race the post-commit getMaterializedView read —
        // treat the missing case as a no-op rather than surfacing NoSuchElementException.
        if (properties.containsKey(SUBSTITUTION_ENABLED)) {
            boolean enabled = (boolean) properties.get(SUBSTITUTION_ENABLED).orElse(false);
            if (enabled) {
                // Distinguish "never refreshed" from "currently fresh" using the connector's
                // freshness check. Never-refreshed MVs (FRESHNESS=STALE with no
                // lastKnownFreshTime) must not be indexed — substituting against an empty
                // storage table would silently return zero rows. Currently-fresh MVs have an
                // empty lastKnownFreshTime by convention (the MV is fresh *now*), so stamp the
                // current time. Stale MVs with a known prior fresh time keep that real time so
                // the grace-period check in MvSubstitutionOptimizer sees the actual age.
                MaterializedViewFreshness freshness = metadata.getMaterializedViewFreshness(session, viewName, false);
                Optional<Instant> indexTime = freshness.getLastKnownFreshTime()
                        .or(() -> freshness.getFreshness() == FRESH ? Optional.of(Instant.now()) : Optional.empty());
                if (indexTime.isEmpty()) {
                    return;
                }
                metadata.getMaterializedView(session, viewName)
                        .flatMap(definition -> create(session, viewName, definition, indexTime.get()))
                        .ifPresent(materializationMetastore::createOrReplace);
            }
            else {
                materializationMetastore.remove(viewName.asCatalogSchemaTableName());
            }
        }
    }

    private Optional<StorageTableId> getStorageTableId(Session session, CatalogSchemaTableName storageTableName)
    {
        TableHandle storageTableHandle = metadata.getTableHandle(session, new QualifiedObjectName(
                        storageTableName.getCatalogName(),
                        storageTableName.getSchemaTableName().getSchemaName(),
                        storageTableName.getSchemaTableName().getTableName()))
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Storage table handle is missing " + storageTableName));

        return substitutionMetadata.getStorageTableId(session, storageTableHandle);
    }

    @Override
    public void finishRefreshMaterializedView(Session session, QualifiedObjectName materializedViewName)
    {
        // REFRESH commits to the connector before this phase; a concurrent DROP can leave the
        // MV gone by the time we observe it. Treat missing as a no-op.
        Optional<MaterializedViewDefinition> maybeDefinition = metadata.getMaterializedView(session, materializedViewName);
        if (maybeDefinition.isEmpty()) {
            return;
        }
        MaterializedViewDefinition definition = maybeDefinition.get();
        Map<String, Object> properties = metadata.getMaterializedViewProperties(session, materializedViewName, definition);
        if (isSubstitutionEnabled(properties)) {
            create(session, materializedViewName, definition, Instant.now())
                    .ifPresent(materializationMetastore::createOrReplace);
        }
    }

    private Optional<MaterializationDefinition> create(
            Session session,
            QualifiedObjectName mvName,
            MaterializedViewDefinition definition,
            Instant lastKnownFreshTime)
    {
        CatalogSchemaTableName storageTableName = definition.getStorageTable()
                .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Materialized View must have storage table to be considered for substitution " + definition));

        // Empty storage identity means the storage table has no snapshot to pin (e.g. it was never
        // refreshed). Skip indexing such a view; no substitution happens until a future refresh
        // produces a snapshot to capture.
        Optional<StorageTableId> storageTableId = getStorageTableId(session, storageTableName);
        if (storageTableId.isEmpty()) {
            return Optional.empty();
        }

        PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, definition.getOriginalSql());
        Query query = (Query) preparedQuery.getStatement();
        Optional<Output> extractedIr = materializationIrExtractor.extract(session, query, preparedQuery.getParameters());

        return extractedIr.map(ir -> new MaterializationDefinition(
                ir,
                storageTableId.get(),
                new MaterializedViewSource(mvName.asCatalogSchemaTableName()),
                lastKnownFreshTime,
                definition.getGracePeriod()));
    }

    @Override
    public void renameIfExists(Session session, QualifiedObjectName source, QualifiedObjectName target)
    {
        // RENAME's connector-level mutation has already committed; a concurrent DROP can leave
        // the (renamed) MV gone before we read it back. Treat missing as a no-op.
        Optional<MaterializedViewDefinition> maybeNewDefinition = metadata.getMaterializedView(session, target);
        if (maybeNewDefinition.isEmpty()) {
            return;
        }
        MaterializedViewDefinition newDefinition = maybeNewDefinition.get();
        if (newDefinition.getStorageTable().isPresent()) {
            CatalogSchemaTableName storageTableName = newDefinition.getStorageTable().get();
            getStorageTableId(session, storageTableName).ifPresent(id ->
                    materializationMetastore.renameIfExists(
                            source.asCatalogSchemaTableName(),
                            target.asCatalogSchemaTableName(),
                            id));
        }
    }

    @Override
    public void remove(QualifiedObjectName materializedViewName)
    {
        materializationMetastore.remove(materializedViewName.asCatalogSchemaTableName());
    }
}
