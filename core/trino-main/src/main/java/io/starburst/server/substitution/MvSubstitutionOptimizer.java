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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.Session;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewReference;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.getMaterializedViewSubstitutionCandidatesRegexFilter;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.getMaterializedViewSubstitutionMaxStaleness;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.isMaterializedViewSubstitutionEnabled;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public class MvSubstitutionOptimizer
        implements PlanOptimizer
{
    // Distinct candidates-regex-filter values are bounded by cluster config and per-session overrides, so a small cache suffices
    private static final long MAX_CACHED_PATTERNS = 100;

    private final MaterializationIndex materializationIndex;
    private final Metadata metadata;
    private final SubstitutionMetadata substitutionMetadata;
    private final AccessControl accessControl;
    private final NonEvictableLoadingCache<String, Pattern> compiledCandidatesRegexFilters;

    public MvSubstitutionOptimizer(MaterializationIndex materializationIndex, Metadata metadata, SubstitutionMetadata substitutionMetadata, AccessControl accessControl)
    {
        this.materializationIndex = requireNonNull(materializationIndex, "materializationIndex is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.compiledCandidatesRegexFilters = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(MAX_CACHED_PATTERNS),
                CacheLoader.from(Pattern::compile));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        if (!isMaterializedViewSubstitutionEnabled(context.session())) {
            return plan;
        }
        // Skip substitution while planning REFRESH MATERIALIZED VIEW: otherwise the MV's defining query
        // could be rewritten to scan the MV's own storage table, turning the refresh into a no-op self-read.
        // Hive MVs that do the refresh outside the engine need to use materialized_view_substitution_enabled=false session property.
        if (isRefreshMaterializedViewPlan(plan)) {
            return plan;
        }
        return optimize(context.session(), plan).orElse(plan);
    }

    private static boolean isRefreshMaterializedViewPlan(PlanNode plan)
    {
        return PlanNodeSearcher.searchFrom(plan)
                .where(node -> node instanceof TableWriterNode writer
                        && writer.getTarget() instanceof RefreshMaterializedViewReference)
                .matches();
    }

    private Optional<PlanNode> optimize(Session session, PlanNode root)
    {
        Map<PlanNode, ComputationHash> computationHashes = calculateHashes(session, root);
        if (computationHashes.isEmpty()) {
            return Optional.empty();
        }
        return optimize(session, root, computationHashes);
    }

    private Optional<PlanNode> optimize(Session session, PlanNode planNode, Map<PlanNode, ComputationHash> computationHashes)
    {
        ComputationHash hash = computationHashes.get(planNode);
        if (hash != null) {
            List<MaterializationDefinition> candidates = materializationIndex.getMaterializations(hash);
            for (MaterializationDefinition candidate : candidates) {
                Optional<PlanNode> substitute = trySubstitute(session, planNode, candidate);
                if (substitute.isPresent()) {
                    return substitute;
                }
            }
        }

        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        boolean anySourceOptimized = false;
        for (PlanNode source : planNode.getSources()) {
            Optional<PlanNode> optimized = optimize(session, source, computationHashes);
            anySourceOptimized |= optimized.isPresent();
            newSources.add(optimized.orElse(source));
        }
        if (!anySourceOptimized) {
            return Optional.empty();
        }

        return Optional.of(planNode.replaceChildren(newSources.build()));
    }

    private Optional<PlanNode> trySubstitute(Session session, PlanNode planNode, MaterializationDefinition candidate)
    {
        if (!(planNode instanceof TableScanNode queryTableScan && substitutionSupported(queryTableScan))) {
            return Optional.empty();
        }

        if (!isFreshEnough(session, candidate)) {
            return Optional.empty();
        }

        if (!matchesCandidatesRegexFilter(session, candidate)) {
            return Optional.empty();
        }

        Optional<MatchingResult> matchingResult = tryMatch(session, planNode, candidate.computationPlanRoot().source());
        if (matchingResult.isEmpty()) {
            return Optional.empty();
        }

        CatalogSchemaTableName storageTable = candidate.storageTableId().tableName();
        Optional<TableHandle> storageTableHandle;
        try {
            storageTableHandle = metadata.getTableHandle(session, new QualifiedObjectName(
                    storageTable.getCatalogName(),
                    storageTable.getSchemaTableName().getSchemaName(),
                    storageTable.getSchemaTableName().getTableName()));
        }
        catch (TableNotFoundException e) {
            // Iceberg throws an error if mv does not exist when we ask for the storage table handle.
            return Optional.empty();
        }
        if (storageTableHandle.isEmpty()) {
            return Optional.empty();
        }

        Optional<StorageTableId> currentStorageTableId = substitutionMetadata.getStorageTableId(session, storageTableHandle.get());
        if (currentStorageTableId.isEmpty() || !currentStorageTableId.get().equals(candidate.storageTableId())) {
            // The storage table that resolves by name today does not report the same id that was captured
            // when this materialization was indexed: its snapshot advanced via a divergent refresh, or the
            // MV was dropped and recreated.
            return Optional.empty();
        }

        Map<String, ColumnHandle> storageColumnHandles = metadata.getColumnHandles(session, storageTableHandle.get());

        Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping = matchingResult.get().querySymbolToMvSymbolMapping();
        ImmutableList.Builder<io.trino.sql.planner.Symbol> outputs = ImmutableList.builder();
        ImmutableMap.Builder<io.trino.sql.planner.Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        Output candidateOutput = candidate.computationPlanRoot();

        Map<Symbol, String> candidateColumnNames = new HashMap<>();
        for (int i = 0; i < candidateOutput.outputs().size(); i++) {
            candidateColumnNames.put(candidateOutput.outputs().get(i), candidateOutput.columnNames().get(i));
        }

        ImmutableSet.Builder<String> readMvColumns = ImmutableSet.builder();
        for (io.trino.sql.planner.Symbol symbol : queryTableScan.getOutputSymbols()) {
            Symbol computationSymbol = querySymbolToMvSymbolMapping.get(symbol);
            String mvColumnName = candidateColumnNames.get(computationSymbol);
            ColumnHandle storageColumnHandle = storageColumnHandles.get(mvColumnName);
            if (storageColumnHandle == null) {
                return Optional.empty();
            }
            outputs.add(symbol);
            assignments.put(symbol, storageColumnHandle);
            readMvColumns.add(mvColumnName);
        }

        // Substitution must not let the user read the materialized view's data without SELECT access to the MV.
        // When access is denied, skip this candidate so the query falls back to other matching MVs,
        // or to the base table (which the user can read).
        if (!canSelectFromMaterializedView(session, candidate, readMvColumns.build())) {
            return Optional.empty();
        }

        return Optional.of(new TableScanNode(
                planNode.getId(),
                storageTableHandle.get(),
                outputs.build(),
                assignments.buildOrThrow(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty()));
    }

    private boolean canSelectFromMaterializedView(Session session, MaterializationDefinition candidate, Set<String> readMvColumns)
    {
        CatalogSchemaTableName mvName = ((MaterializedViewSource) candidate.source()).materializedViewName();
        QualifiedObjectName materializedView = new QualifiedObjectName(
                mvName.getCatalogName(),
                mvName.getSchemaTableName().getSchemaName(),
                mvName.getSchemaTableName().getTableName());
        try {
            accessControl.checkCanSelectFromColumns(session.toSecurityContext(), materializedView, Optional.empty(), readMvColumns);
            return true;
        }
        catch (AccessDeniedException _) {
            return false;
        }
    }

    private Optional<MatchingResult> tryMatch(Session session, PlanNode planNode, Operation operation)
    {
        MatchingVisitor visitor = new MatchingVisitor(substitutionMetadata, session);
        if (!planNode.accept(visitor, operation)) {
            return Optional.empty();
        }
        return Optional.of(new MatchingResult(visitor.buildQuerySymbolToMvSymbolMapping()));
    }

    private static boolean isFreshEnough(Session session, MaterializationDefinition candidate)
    {
        Duration sinceRefresh = Duration.between(candidate.lastKnownFreshTime(), session.getStart());
        boolean withinGracePeriod = candidate.gracePeriod().isEmpty()
                || sinceRefresh.compareTo(candidate.gracePeriod().get()) <= 0;
        boolean withinMaxStaleness = getMaterializedViewSubstitutionMaxStaleness(session)
                .map(maxStaleness -> sinceRefresh.compareTo(maxStaleness.toJavaTime()) <= 0)
                .orElse(true);
        return withinGracePeriod && withinMaxStaleness;
    }

    private boolean matchesCandidatesRegexFilter(Session session, MaterializationDefinition candidate)
    {
        Optional<String> candidatesRegexFilter = getMaterializedViewSubstitutionCandidatesRegexFilter(session);
        if (candidatesRegexFilter.isEmpty()) {
            return true;
        }
        Pattern pattern = compiledCandidatesRegexFilters.getUnchecked(candidatesRegexFilter.get());
        CatalogSchemaTableName materializedViewName = ((MaterializedViewSource) candidate.source()).materializedViewName();
        return pattern.matcher(materializedViewName.toString()).matches();
    }

    private boolean substitutionSupported(TableScanNode tableScan)
    {
        return !tableScan.isUpdateTarget() && tableScan.getEnforcedConstraint().isAll() && tableScan.getUseConnectorNodePartitioning().isEmpty();
    }

    private Map<PlanNode, ComputationHash> calculateHashes(Session session, PlanNode root)
    {
        ImmutableMap.Builder<PlanNode, ComputationHash> result = ImmutableMap.builder();
        root.accept(new CalculateHashVisitor(substitutionMetadata, session), result);
        return result.buildOrThrow();
    }

    private static class CalculateHashVisitor
            extends PlanVisitor<Optional<ComputationHash>, ImmutableMap.Builder<PlanNode, ComputationHash>>
    {
        private final SubstitutionMetadata substitutionMetadata;
        private final Session session;

        private CalculateHashVisitor(SubstitutionMetadata substitutionMetadata, Session session)
        {
            this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Optional<ComputationHash> visitPlan(PlanNode node, ImmutableMap.Builder<PlanNode, ComputationHash> context)
        {
            node.getSources().forEach(source -> source.accept(this, context));
            return Optional.empty();
        }

        @Override
        public Optional<ComputationHash> visitTableScan(TableScanNode node, ImmutableMap.Builder<PlanNode, ComputationHash> context)
        {
            Optional<TableId> tableId = substitutionMetadata.getTableId(session, node.getTable());
            return tableId.map(id -> {
                ComputationHash hash = new ComputationHash(id.hash());
                context.put(node, hash);
                return hash;
            });
        }
    }

    private static class MatchingVisitor
            extends PlanVisitor<Boolean, Operation>
    {
        private final SubstitutionMetadata substitutionMetadata;
        private final Session session;
        private final ImmutableMap.Builder<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping = ImmutableMap.builder();

        private MatchingVisitor(SubstitutionMetadata substitutionMetadata, Session session)
        {
            this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Boolean visitPlan(PlanNode node, Operation materializationComputation)
        {
            return false;
        }

        @Override
        public Boolean visitTableScan(TableScanNode node, Operation materializationComputation)
        {
            if (!(materializationComputation instanceof TableScan materializationTableScan)) {
                return false;
            }

            if (!substitutionMetadata.tableHandleMatchesId(session, node.getTable(), materializationTableScan.table())) {
                return false;
            }

            Map<ConnectorColumnId, Symbol> mvAssignments = materializationTableScan.assignments();
            for (Map.Entry<io.trino.sql.planner.Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Optional<ConnectorColumnId> columnId = substitutionMetadata.getColumnId(session, node.getTable(), entry.getValue());
                if (columnId.isEmpty()) {
                    return false;
                }
                Symbol mvSymbol = mvAssignments.get(columnId.get());
                if (mvSymbol == null) {
                    return false;
                }
                querySymbolToMvSymbolMapping.put(entry.getKey(), mvSymbol);
            }
            return true;
        }

        private Map<io.trino.sql.planner.Symbol, Symbol> buildQuerySymbolToMvSymbolMapping()
        {
            return querySymbolToMvSymbolMapping.buildOrThrow();
        }
    }

    record MatchingResult(Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping) {}
}
