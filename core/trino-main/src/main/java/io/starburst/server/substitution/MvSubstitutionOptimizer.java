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
import io.starburst.server.substitution.MaterializedViewSubstitution.ReplacedPlan;
import io.trino.Session;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.execution.TableInfo;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Cast;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewReference;
import io.trino.sql.planner.planprinter.TableInfoSupplier;
import io.trino.type.TypeCoercion;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.getMaterializedViewSubstitutionCandidatesRegexFilter;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.getMaterializedViewSubstitutionMaxStaleness;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.isMaterializedViewSubstitutionEnabled;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.ACCESS_DENIED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_ACCESS_DENIED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_NOT_IN_MATERIALIZED_VIEW;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COLUMN_TYPE_MISMATCH;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.COMPUTATION_MISMATCH;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.EXCLUDED_BY_FILTER;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.MAX_STALENESS_EXCEEDED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.SCAN_NOT_SUBSTITUTABLE;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STALE;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_COLUMN_MISSING;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TABLE_CHANGED;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TABLE_NOT_FOUND;
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.STORAGE_TYPE_NOT_COERCIBLE;
import static java.util.Objects.requireNonNull;

public class MvSubstitutionOptimizer
        implements PlanOptimizer
{
    // Distinct candidates-regex-filter values are bounded by cluster config and per-session overrides, so a small cache suffices
    private static final long MAX_CACHED_PATTERNS = 100;

    private final MaterializationIndex materializationIndex;
    private final Metadata metadata;
    private final SubstitutionMetadata substitutionMetadata;
    private final TypeManager typeManager;
    private final AccessControl accessControl;
    private final NonEvictableLoadingCache<String, Pattern> compiledCandidatesRegexFilters;

    public MvSubstitutionOptimizer(
            MaterializationIndex materializationIndex,
            Metadata metadata,
            SubstitutionMetadata substitutionMetadata,
            TypeManager typeManager,
            AccessControl accessControl)
    {
        this.materializationIndex = requireNonNull(materializationIndex, "materializationIndex is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.typeManager = typeManager;
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
        TypeCoercion typeCoercion = new TypeCoercion(typeManager::getType, getCharVarcharCoercion(context.session()));
        return optimize(context, plan, typeCoercion).orElse(plan);
    }

    private static boolean isRefreshMaterializedViewPlan(PlanNode plan)
    {
        return PlanNodeSearcher.searchFrom(plan)
                .where(node -> node instanceof TableWriterNode writer
                        && writer.getTarget() instanceof RefreshMaterializedViewReference)
                .matches();
    }

    private Optional<PlanNode> optimize(Context context, PlanNode root, TypeCoercion typeCoercion)
    {
        Map<PlanNode, ComputationHash> computationHashes = calculateHashes(context.session(), root);
        if (computationHashes.isEmpty()) {
            return Optional.empty();
        }
        return optimize(context, root, computationHashes, typeCoercion);
    }

    private Optional<PlanNode> optimize(Context context, PlanNode planNode, Map<PlanNode, ComputationHash> computationHashes, TypeCoercion typeCoercion)
    {
        ComputationHash hash = computationHashes.get(planNode);
        if (hash != null && planNode instanceof TableScanNode queryTableScan) {
            Optional<PlanNode> substituted = trySubstituteAgainstCandidates(context, queryTableScan, hash, typeCoercion);
            if (substituted.isPresent()) {
                return substituted;
            }
        }

        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        boolean anySourceOptimized = false;
        for (PlanNode source : planNode.getSources()) {
            Optional<PlanNode> optimized = optimize(context, source, computationHashes, typeCoercion);
            anySourceOptimized |= optimized.isPresent();
            newSources.add(optimized.orElse(source));
        }
        if (!anySourceOptimized) {
            return Optional.empty();
        }

        return Optional.of(planNode.replaceChildren(newSources.build()));
    }

    private Optional<PlanNode> trySubstituteAgainstCandidates(Context context, TableScanNode queryTableScan, ComputationHash hash, TypeCoercion typeCoercion)
    {
        MvSubstitutionCollector collector = context.mvSubstitutionCollector();
        Session session = context.session();

        List<MaterializationDefinition> candidates = materializationIndex.getMaterializations(hash);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        // SCAN_NOT_SUBSTITUTABLE is a near miss only where a materialization covers this computation: a scan
        // no MV materializes could never have been substituted, so it is neither recorded nor counted.
        if (!substitutionSupported(queryTableScan)) {
            recordRejectedScanLevel(collector, SCAN_NOT_SUBSTITUTABLE);
            return Optional.empty();
        }

        for (MaterializationDefinition candidate : candidates) {
            SubstitutionAttempt attempt = trySubstitute(context, queryTableScan, candidate, typeCoercion);
            CatalogSchemaTableName candidateMvName = materializedViewName(candidate);
            switch (attempt) {
                case SubstitutionAttempt.Substituted substituted -> {
                    recordSubstituted(collector, session, queryTableScan, substituted);
                    return Optional.of(substituted.node());
                }
                case SubstitutionAttempt.Rejected rejected -> recordRejectedCandidate(collector, candidateMvName, rejected.reason());
            }
        }
        return Optional.empty();
    }

    private void recordSubstituted(MvSubstitutionCollector collector, Session session, TableScanNode queryTableScan, SubstitutionAttempt.Substituted substituted)
    {
        ReplacedPlan replacedPlan = new ReplacedPlan(queryTableScan, captureTableInfos(session, queryTableScan));
        collector.recordSubstituted(substituted.materializedView(), substituted.storageTable(), substituted.lastRefreshTime(), replacedPlan);
    }

    private Map<PlanNodeId, TableInfo> captureTableInfos(Session session, PlanNode subtree)
    {
        TableInfoSupplier tableInfoSupplier = new TableInfoSupplier(metadata, session);
        return PlanNodeSearcher.searchFrom(subtree)
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableMap(TableScanNode::getId, tableInfoSupplier));
    }

    private void recordRejectedCandidate(MvSubstitutionCollector collector, CatalogSchemaTableName candidateMvName, MaterializedViewSubstitutionOutcome outcome)
    {
        // Naming the MV in an ACCESS_DENIED row would disclose an object the user cannot read.
        // Every other outcome is reached only after the user passed the table-level access check.
        Optional<CatalogSchemaTableName> recordedMvName = outcome == ACCESS_DENIED ? Optional.empty() : Optional.of(candidateMvName);
        collector.recordRejected(recordedMvName, outcome);
    }

    private void recordRejectedScanLevel(MvSubstitutionCollector collector, MaterializedViewSubstitutionOutcome outcome)
    {
        collector.recordRejected(Optional.empty(), outcome);
    }

    private static CatalogSchemaTableName materializedViewName(MaterializationDefinition candidate)
    {
        return ((MaterializedViewSource) candidate.source()).materializedViewName();
    }

    private SubstitutionAttempt trySubstitute(Context context, TableScanNode queryTableScan, MaterializationDefinition candidate, TypeCoercion typeCoercion)
    {
        Session session = context.session();
        // A rejection records the candidate's name, so the table-level access check comes before every other
        // rejection reason: a user who cannot read the MV must not learn from their query's recorded rows that
        // it exists and matches their query. The column-level check follows once the read columns are known.
        if (!canSelectFromMaterializedView(session, candidate, ImmutableSet.of())) {
            return new SubstitutionAttempt.Rejected(ACCESS_DENIED);
        }

        if (!isWithinGracePeriod(session, candidate)) {
            return new SubstitutionAttempt.Rejected(STALE);
        }

        if (!isWithinMaxStaleness(session, candidate)) {
            return new SubstitutionAttempt.Rejected(MAX_STALENESS_EXCEEDED);
        }

        if (!matchesCandidatesRegexFilter(session, candidate)) {
            return new SubstitutionAttempt.Rejected(EXCLUDED_BY_FILTER);
        }

        MatchOutcome matchOutcome = tryMatch(session, queryTableScan, candidate.computationPlanRoot().source());
        if (matchOutcome.result().isEmpty()) {
            return new SubstitutionAttempt.Rejected(matchOutcome.rejectionReason().orElseThrow());
        }
        MatchingResult matchingResult = matchOutcome.result().get();

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
            return new SubstitutionAttempt.Rejected(STORAGE_TABLE_NOT_FOUND);
        }
        if (storageTableHandle.isEmpty()) {
            return new SubstitutionAttempt.Rejected(STORAGE_TABLE_NOT_FOUND);
        }

        Optional<StorageTableId> currentStorageTableId = substitutionMetadata.getStorageTableId(session, storageTableHandle.get());
        if (currentStorageTableId.isEmpty() || !currentStorageTableId.get().equals(candidate.storageTableId())) {
            // The storage table that resolves by name today does not report the same id that was captured
            // when this materialization was indexed: its snapshot advanced via a divergent refresh, or the
            // MV was dropped and recreated.
            return new SubstitutionAttempt.Rejected(STORAGE_TABLE_CHANGED);
        }

        Map<String, ColumnHandle> storageColumnHandles = metadata.getColumnHandles(session, storageTableHandle.get());

        Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping = matchingResult.querySymbolToMvSymbolMapping();
        ImmutableList.Builder<io.trino.sql.planner.Symbol> outputs = ImmutableList.builder();
        ImmutableMap.Builder<io.trino.sql.planner.Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        Output candidateOutput = candidate.computationPlanRoot();

        Map<Symbol, String> candidateColumnNames = new HashMap<>();
        for (int i = 0; i < candidateOutput.outputs().size(); i++) {
            candidateColumnNames.put(candidateOutput.outputs().get(i), candidateOutput.columnNames().get(i));
        }

        ImmutableSet.Builder<String> readMvColumns = ImmutableSet.builder();
        Assignments.Builder projections = Assignments.builder();
        boolean needsCoercion = false;
        for (io.trino.sql.planner.Symbol symbol : queryTableScan.getOutputSymbols()) {
            Symbol computationSymbol = querySymbolToMvSymbolMapping.get(symbol);
            String mvColumnName = candidateColumnNames.get(computationSymbol);
            ColumnHandle storageColumnHandle = storageColumnHandles.get(mvColumnName);
            if (storageColumnHandle == null) {
                return new SubstitutionAttempt.Rejected(STORAGE_COLUMN_MISSING);
            }
            readMvColumns.add(mvColumnName);

            Type storageType = metadata.getColumnMetadata(session, storageTableHandle.get(), storageColumnHandle).getType();
            if (!(typeCoercion.canCoerce(symbol.type(), storageType) || (
            // VARCHAR is not strictly coercible to CHAR, but in this we can safely do it
            // as the source of the data was CHAR in the first place
                    storageType instanceof VarcharType && symbol.type() instanceof CharType))) {
                return new SubstitutionAttempt.Rejected(STORAGE_TYPE_NOT_COERCIBLE);
            }

            if (storageType.equals(symbol.type())) {
                outputs.add(symbol);
                assignments.put(symbol, storageColumnHandle);
                projections.put(symbol, symbol.toSymbolReference());
            }
            else {
                // The storage column type can differ from the query column type by a type-only coercion:
                // e.g. the source table has varchar(10) while the Iceberg storage table has unbounded
                // varchar. Scan the storage type and cast up to the query type in a projection above the
                // scan, so the substituted subtree still produces the exact types the rest of the plan expects.
                io.trino.sql.planner.Symbol scanSymbol = context.symbolAllocator().newSymbol(symbol.name(), storageType);
                outputs.add(scanSymbol);
                assignments.put(scanSymbol, storageColumnHandle);
                projections.put(symbol, new Cast(scanSymbol.toSymbolReference(), symbol.type()));
                needsCoercion = true;
            }
        }

        // Substitution must not let the user read the materialized view's data without SELECT access to the
        // columns it reads. When access is denied, skip this candidate so the query falls back to other
        // matching MVs, or to the base table (which the user can read).
        if (!canSelectFromMaterializedView(session, candidate, readMvColumns.build())) {
            return new SubstitutionAttempt.Rejected(COLUMN_ACCESS_DENIED);
        }

        TableScanNode storageScan = new TableScanNode(
                needsCoercion ? context.idAllocator().getNextId() : queryTableScan.getId(),
                storageTableHandle.get(),
                outputs.build(),
                assignments.buildOrThrow(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        PlanNode resultNode = storageScan;
        if (needsCoercion) {
            resultNode = new ProjectNode(queryTableScan.getId(), storageScan, projections.build());
        }
        return new SubstitutionAttempt.Substituted(
                resultNode,
                materializedViewName(candidate),
                storageTable,
                candidate.lastKnownFreshTime());
    }

    private boolean canSelectFromMaterializedView(Session session, MaterializationDefinition candidate, Set<String> readMvColumns)
    {
        CatalogSchemaTableName mvName = materializedViewName(candidate);
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

    private MatchOutcome tryMatch(Session session, PlanNode planNode, Operation operation)
    {
        MatchingVisitor visitor = new MatchingVisitor(substitutionMetadata, session);
        if (!planNode.accept(visitor, operation)) {
            return new MatchOutcome(Optional.empty(), Optional.of(visitor.outcome()));
        }
        return new MatchOutcome(Optional.of(new MatchingResult(visitor.buildQuerySymbolToMvSymbolMapping())), Optional.empty());
    }

    private static boolean isWithinGracePeriod(Session session, MaterializationDefinition candidate)
    {
        Duration sinceRefresh = Duration.between(candidate.lastKnownFreshTime(), session.getStart());
        return candidate.gracePeriod().isEmpty()
                || sinceRefresh.compareTo(candidate.gracePeriod().get()) <= 0;
    }

    private static boolean isWithinMaxStaleness(Session session, MaterializationDefinition candidate)
    {
        Duration sinceRefresh = Duration.between(candidate.lastKnownFreshTime(), session.getStart());
        return getMaterializedViewSubstitutionMaxStaleness(session)
                .map(maxStaleness -> sinceRefresh.compareTo(maxStaleness.toJavaTime()) <= 0)
                .orElse(true);
    }

    private boolean matchesCandidatesRegexFilter(Session session, MaterializationDefinition candidate)
    {
        Optional<String> candidatesRegexFilter = getMaterializedViewSubstitutionCandidatesRegexFilter(session);
        if (candidatesRegexFilter.isEmpty()) {
            return true;
        }
        Pattern pattern = compiledCandidatesRegexFilters.getUnchecked(candidatesRegexFilter.get());
        CatalogSchemaTableName materializedViewName = materializedViewName(candidate);
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
        private MaterializedViewSubstitutionOutcome outcome = COMPUTATION_MISMATCH;

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
                    outcome = COLUMN_NOT_IN_MATERIALIZED_VIEW;
                    return false;
                }
                if (!mvSymbol.type().equals(entry.getKey().type())) {
                    outcome = COLUMN_TYPE_MISMATCH;
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

        private MaterializedViewSubstitutionOutcome outcome()
        {
            return outcome;
        }
    }

    private record MatchingResult(Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping) {}

    private record MatchOutcome(Optional<MatchingResult> result, Optional<MaterializedViewSubstitutionOutcome> rejectionReason)
    {
        MatchOutcome
        {
            checkArgument(result.isPresent() != rejectionReason.isPresent(), "exactly one of result or rejectionReason must be present");
        }
    }

    sealed interface SubstitutionAttempt
    {
        record Substituted(PlanNode node, CatalogSchemaTableName materializedView, CatalogSchemaTableName storageTable, Instant lastRefreshTime)
                implements SubstitutionAttempt {}

        record Rejected(MaterializedViewSubstitutionOutcome reason)
                implements SubstitutionAttempt {}
    }
}
