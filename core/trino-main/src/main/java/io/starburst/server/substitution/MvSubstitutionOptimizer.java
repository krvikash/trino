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
import io.starburst.materialization.ir.Aggregation;
import io.starburst.materialization.ir.Aggregation.GroupingSets;
import io.starburst.materialization.ir.Aggregation.SortItem;
import io.starburst.materialization.ir.Expression;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Reference;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.starburst.server.substitution.MaterializationIndex.IndexedMaterialization;
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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.server.substitution.MaterializationIrExtractor.toIrStep;
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
import static io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome.SUBSTITUTED;
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
    private final MvSubstitutionStats mvSubstitutionStats;
    private final MvSubstitutionUsageMetrics mvSubstitutionUsageMetrics;
    private final NonEvictableLoadingCache<String, Pattern> compiledCandidatesRegexFilters;

    public MvSubstitutionOptimizer(
            MaterializationIndex materializationIndex,
            Metadata metadata,
            SubstitutionMetadata substitutionMetadata,
            TypeManager typeManager,
            AccessControl accessControl,
            MvSubstitutionStats mvSubstitutionStats,
            MvSubstitutionUsageMetrics mvSubstitutionUsageMetrics)
    {
        this.materializationIndex = requireNonNull(materializationIndex, "materializationIndex is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
        this.typeManager = typeManager;
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.mvSubstitutionStats = requireNonNull(mvSubstitutionStats, "mvSubstitutionStats is null");
        this.mvSubstitutionUsageMetrics = requireNonNull(mvSubstitutionUsageMetrics, "mvSubstitutionUsageMetrics is null");
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
        // Column ids depend only on the query, so they are resolved once for every node and candidate
        QueryColumnIds queryColumnIds = new QueryColumnIds(substitutionMetadata, context.session());
        return optimize(context, root, computationHashes, typeCoercion, queryColumnIds);
    }

    private Optional<PlanNode> optimize(Context context, PlanNode planNode, Map<PlanNode, ComputationHash> computationHashes, TypeCoercion typeCoercion, QueryColumnIds queryColumnIds)
    {
        ComputationHash hash = computationHashes.get(planNode);
        if (hash != null) {
            Optional<PlanNode> substituted = trySubstituteAgainstCandidates(context, planNode, hash, typeCoercion, queryColumnIds);
            if (substituted.isPresent()) {
                return substituted;
            }
        }

        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        boolean anySourceOptimized = false;
        for (PlanNode source : planNode.getSources()) {
            Optional<PlanNode> optimized = optimize(context, source, computationHashes, typeCoercion, queryColumnIds);
            anySourceOptimized |= optimized.isPresent();
            newSources.add(optimized.orElse(source));
        }
        if (!anySourceOptimized) {
            return Optional.empty();
        }

        return Optional.of(planNode.replaceChildren(newSources.build()));
    }

    private Optional<PlanNode> trySubstituteAgainstCandidates(Context context, PlanNode queryNode, ComputationHash hash, TypeCoercion typeCoercion, QueryColumnIds queryColumnIds)
    {
        MvSubstitutionCollector collector = context.mvSubstitutionCollector();
        Session session = context.session();

        List<IndexedMaterialization> candidates = materializationIndex.getIndexedMaterializations(hash);
        if (candidates.isEmpty()) {
            return Optional.empty();
        }

        // SCAN_NOT_SUBSTITUTABLE is a near miss only where a materialization covers this computation: a scan
        // no MV materializes could never have been substituted, so it is neither recorded nor counted.
        Optional<MaterializedViewSubstitutionOutcome> unsupported = queryNode.accept(new SubstitutionSupportedVisitor(), null);
        if (unsupported.isPresent()) {
            recordRejectedScanLevel(collector, unsupported.get());
            return Optional.empty();
        }

        for (IndexedMaterialization candidate : candidates) {
            SubstitutionAttempt attempt = trySubstitute(context, queryNode, candidate.definition(), candidate.columnNames(), typeCoercion, queryColumnIds);
            CatalogSchemaTableName candidateMvName = materializedViewName(candidate.definition());
            switch (attempt) {
                case SubstitutionAttempt.Substituted substituted -> {
                    recordSubstituted(collector, session, queryNode, substituted);
                    return Optional.of(substituted.node());
                }
                case SubstitutionAttempt.Rejected rejected -> recordRejectedCandidate(collector, candidateMvName, rejected.reason());
            }
        }
        return Optional.empty();
    }

    private void recordSubstituted(MvSubstitutionCollector collector, Session session, PlanNode queryNode, SubstitutionAttempt.Substituted substituted)
    {
        ReplacedPlan replacedPlan = new ReplacedPlan(queryNode, captureTableInfos(session, queryNode));
        collector.recordSubstituted(substituted.materializedView(), substituted.storageTable(), substituted.lastRefreshTime(), replacedPlan);
        recordOutcome(SUBSTITUTED);
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
        // Naming the MV in an ACCESS_DENIED row would disclose an object the user cannot read at all.
        // Every other outcome, COLUMN_ACCESS_DENIED included, is reached only after the user passed
        // that table-level access check, so the name is safe to record there.
        Optional<CatalogSchemaTableName> recordedMvName = outcome == ACCESS_DENIED ? Optional.empty() : Optional.of(candidateMvName);
        collector.recordRejected(recordedMvName, outcome);
        recordOutcome(outcome);
    }

    private void recordRejectedScanLevel(MvSubstitutionCollector collector, MaterializedViewSubstitutionOutcome outcome)
    {
        collector.recordRejected(Optional.empty(), outcome);
        recordOutcome(outcome);
    }

    private void recordOutcome(MaterializedViewSubstitutionOutcome outcome)
    {
        mvSubstitutionStats.increment(outcome);
        mvSubstitutionUsageMetrics.recordAttempt(outcome);
    }

    private static CatalogSchemaTableName materializedViewName(MaterializationDefinition candidate)
    {
        return ((MaterializedViewSource) candidate.source()).materializedViewName();
    }

    private SubstitutionAttempt trySubstitute(
            Context context,
            PlanNode queryNode,
            MaterializationDefinition candidate,
            Map<Symbol, String> candidateColumnNames,
            TypeCoercion typeCoercion,
            QueryColumnIds queryColumnIds)
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

        MatchOutcome matchOutcome = tryMatch(session, queryNode, candidate.computationPlanRoot(), queryColumnIds);
        if (matchOutcome.result().isEmpty()) {
            return new SubstitutionAttempt.Rejected(matchOutcome.rejectionReason().orElseThrow());
        }
        MatchingResult matchingResult = matchOutcome.result().get();

        Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping = matchingResult.querySymbolToMvSymbolMapping();
        // An aggregation may list a grouping key twice, e.g. the one SingleDistinctAggregationToGroupBy builds for
        // count(DISTINCT k) GROUP BY k; a scan produces each symbol once, which serves its parents the same
        Map<io.trino.sql.planner.Symbol, String> mvColumnNames = new LinkedHashMap<>();
        for (io.trino.sql.planner.Symbol symbol : queryNode.getOutputSymbols()) {
            String mvColumnName = candidateColumnNames.get(querySymbolToMvSymbolMapping.get(symbol));
            if (mvColumnName == null) {
                // The MV computes this value but does not select it, e.g. a key it groups by
                return new SubstitutionAttempt.Rejected(COLUMN_NOT_IN_MATERIALIZED_VIEW);
            }
            mvColumnNames.put(symbol, mvColumnName);
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

        ImmutableList.Builder<io.trino.sql.planner.Symbol> outputs = ImmutableList.builder();
        ImmutableMap.Builder<io.trino.sql.planner.Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        ImmutableSet.Builder<String> readMvColumns = ImmutableSet.builder();
        Assignments.Builder projections = Assignments.builder();
        boolean needsCoercion = false;
        for (Map.Entry<io.trino.sql.planner.Symbol, String> entry : mvColumnNames.entrySet()) {
            io.trino.sql.planner.Symbol symbol = entry.getKey();
            String mvColumnName = entry.getValue();
            ColumnHandle storageColumnHandle = storageColumnHandles.get(mvColumnName);
            if (storageColumnHandle == null) {
                return new SubstitutionAttempt.Rejected(STORAGE_COLUMN_MISSING);
            }
            readMvColumns.add(mvColumnName);

            Type storageType = metadata.getColumnMetadata(session, storageTableHandle.get(), storageColumnHandle).getType();
            if (!(typeCoercion.canCoerce(symbol.type(), storageType) || (
            // VARCHAR is not strictly coercible to CHAR, but in this case we can safely do it
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
                // The storage column type can differ from the query column type because the Iceberg storage
                // table cannot represent every Trino type: varchar(10) is stored as unbounded varchar, and types
                // such as char(n) or smallint are rewritten to varchar or integer by Iceberg's
                // AbstractTrinoCatalog.typeForMaterializedViewStorageTable. The matcher rejects with
                // COLUMN_TYPE_MISMATCH unless the MV read the base column as symbol.type(), so every stored value
                // originated as that type. Scan the storage type and cast back to symbol.type() in a projection
                // above the scan; the cast loses nothing that was stored.
                io.trino.sql.planner.Symbol scanSymbol = context.symbolAllocator().newSymbol(symbol.name(), storageType);
                outputs.add(scanSymbol);
                assignments.put(scanSymbol, storageColumnHandle);
                projections.put(symbol, new Cast(scanSymbol.toSymbolReference(), symbol.type()));
                needsCoercion = true;
            }
        }

        // Substitution must not let the user read the materialized view's data without SELECT access to the
        // columns it reads. When access is denied, skip this candidate so the query falls back to other
        // matching MVs, or to the base table (which the user can read). Recorded as a distinct outcome
        // from the table-level check above: the user already passed that check, so naming the MV here
        // discloses nothing they couldn't already see.
        if (!canSelectFromMaterializedView(session, candidate, readMvColumns.build())) {
            return new SubstitutionAttempt.Rejected(COLUMN_ACCESS_DENIED);
        }

        TableScanNode storageScan = new TableScanNode(
                needsCoercion ? context.idAllocator().getNextId() : queryNode.getId(),
                storageTableHandle.get(),
                outputs.build(),
                assignments.buildOrThrow(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());
        PlanNode resultNode = storageScan;
        if (needsCoercion) {
            resultNode = new ProjectNode(queryNode.getId(), storageScan, projections.build());
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

    private MatchOutcome tryMatch(Session session, PlanNode planNode, Output candidateOutput, QueryColumnIds queryColumnIds)
    {
        MatchingVisitor visitor = new MatchingVisitor(substitutionMetadata, session, queryColumnIds, candidateOutput.outputs());
        if (!planNode.accept(visitor, candidateOutput.source())) {
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

    private static boolean substitutionSupported(TableScanNode tableScan)
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

        @Override
        public Optional<ComputationHash> visitAggregation(AggregationNode node, ImmutableMap.Builder<PlanNode, ComputationHash> context)
        {
            return node.getSource().accept(this, context).map(sourceHash -> {
                ComputationHash hash = sourceHash.aggregateOf();
                context.put(node, hash);
                return hash;
            });
        }
    }

    /**
     * Reports why a hashed subtree cannot be substituted at all, or nothing when it can: every scan in it must be
     * eligible. Only scans and aggregations over them are hashed.
     */
    private static class SubstitutionSupportedVisitor
            extends PlanVisitor<Optional<MaterializedViewSubstitutionOutcome>, Void>
    {
        @Override
        protected Optional<MaterializedViewSubstitutionOutcome> visitPlan(PlanNode node, Void context)
        {
            return Optional.of(COMPUTATION_MISMATCH);
        }

        @Override
        public Optional<MaterializedViewSubstitutionOutcome> visitTableScan(TableScanNode node, Void context)
        {
            if (!substitutionSupported(node)) {
                return Optional.of(SCAN_NOT_SUBSTITUTABLE);
            }
            return Optional.empty();
        }

        @Override
        public Optional<MaterializedViewSubstitutionOutcome> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }
    }

    private static class MatchingVisitor
            extends PlanVisitor<Boolean, Operation>
    {
        private final SubstitutionMetadata substitutionMetadata;
        private final Session session;
        private final QueryColumnIds queryColumnIds;
        private final Set<Symbol> selectedMvSymbols;
        // Each MV symbol is bound once in its tree, so it identifies a binding; names are never compared across the two plans
        private final Map<io.trino.sql.planner.Symbol, Symbol> querySymbolToMvSymbolMapping = new HashMap<>();
        private MaterializedViewSubstitutionOutcome outcome = COMPUTATION_MISMATCH;

        private MatchingVisitor(SubstitutionMetadata substitutionMetadata, Session session, QueryColumnIds queryColumnIds, List<Symbol> selectedMvSymbols)
        {
            this.queryColumnIds = requireNonNull(queryColumnIds, "queryColumnIds is null");
            this.selectedMvSymbols = ImmutableSet.copyOf(requireNonNull(selectedMvSymbols, "selectedMvSymbols is null"));
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
                Optional<ConnectorColumnId> columnId = queryColumnIds.get(node.getTable(), entry.getValue());
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
                if (!bind(entry.getKey(), mvSymbol)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Operation materializationComputation)
        {
            if (!(materializationComputation instanceof Aggregation materializationAggregate)) {
                return false;
            }
            if (!node.getSource().accept(this, materializationAggregate.source())) {
                return false;
            }

            GroupingSets groupingSets = materializationAggregate.groupingSets();
            if (toIrStep(node.getStep()) != materializationAggregate.step()
                    || node.getGroupingSetCount() != groupingSets.groupingSetCount()
                    || !node.getGlobalGroupingSets().equals(groupingSets.globalGroupingSets())
                    || !mapsTo(node.getGroupIdSymbol(), materializationAggregate.groupIdSymbol())) {
                return false;
            }

            Set<Symbol> mappedGroupingKeys = new HashSet<>();
            for (io.trino.sql.planner.Symbol groupingKey : node.getGroupingKeys()) {
                Symbol mvSymbol = querySymbolToMvSymbolMapping.get(groupingKey);
                if (mvSymbol == null) {
                    return false;
                }
                mappedGroupingKeys.add(mvSymbol);
            }
            if (!mappedGroupingKeys.equals(ImmutableSet.copyOf(groupingSets.groupingKeys()))) {
                return false;
            }

            // The MV may compute more aggregations than the query uses: with the same grouping, its rows are the query's rows
            for (Map.Entry<io.trino.sql.planner.Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                // Equivalent calls differ only in their output symbol: prefer one the MV selects. A nested
                // aggregate's outputs are read by the aggregate above, not selected, so any match serves there
                Symbol mvOutput = null;
                for (Aggregation.AggregateFunctionCall mvAggregation : materializationAggregate.aggregations()) {
                    if (matches(entry.getValue(), mvAggregation)) {
                        if (selectedMvSymbols.contains(mvAggregation.output())) {
                            mvOutput = mvAggregation.output();
                            break;
                        }
                        if (mvOutput == null) {
                            mvOutput = mvAggregation.output();
                        }
                    }
                }
                if (mvOutput == null || !bind(entry.getKey(), mvOutput)) {
                    return false;
                }
            }
            return true;
        }

        private boolean matches(AggregationNode.Aggregation queryAggregation, Aggregation.AggregateFunctionCall mvAggregation)
        {
            if (!queryAggregation.getResolvedFunction().signature().equals(mvAggregation.function())
                    || queryAggregation.isDistinct() != mvAggregation.distinct()
                    || queryAggregation.getArguments().size() != mvAggregation.arguments().size()
                    || !mapsTo(queryAggregation.getFilter(), mvAggregation.filter())
                    || !mapsTo(queryAggregation.getMask(), mvAggregation.mask())
                    || !orderingMatches(queryAggregation.getOrderingScheme(), mvAggregation.orderingScheme())) {
                return false;
            }
            for (int i = 0; i < queryAggregation.getArguments().size(); i++) {
                if (!argumentMatches(queryAggregation.getArguments().get(i), mvAggregation.arguments().get(i))) {
                    return false;
                }
            }
            return true;
        }

        private boolean argumentMatches(io.trino.sql.ir.Expression queryArgument, Expression mvArgument)
        {
            // A lambda argument has no persisted form yet
            if (!(queryArgument instanceof io.trino.sql.ir.Reference queryReference)) {
                return false;
            }
            return mvArgument instanceof Reference mvReference
                    && mapsTo(io.trino.sql.planner.Symbol.from(queryReference), mvReference.symbol());
        }

        private boolean orderingMatches(Optional<OrderingScheme> queryOrdering, Optional<Aggregation.OrderingScheme> mvOrdering)
        {
            if (queryOrdering.isEmpty() || mvOrdering.isEmpty()) {
                return queryOrdering.isEmpty() && mvOrdering.isEmpty();
            }
            List<io.trino.sql.planner.Symbol> orderBy = queryOrdering.get().orderBy();
            List<SortItem> sortItems = mvOrdering.get().sortItems();
            if (orderBy.size() != sortItems.size()) {
                return false;
            }
            for (int i = 0; i < orderBy.size(); i++) {
                SortItem sortItem = sortItems.get(i);
                if (!mapsTo(orderBy.get(i), sortItem.symbol()) || queryOrdering.get().ordering(orderBy.get(i)) != sortItem.sortOrder()) {
                    return false;
                }
            }
            return true;
        }

        private boolean mapsTo(Optional<io.trino.sql.planner.Symbol> querySymbol, Optional<Symbol> mvSymbol)
        {
            if (querySymbol.isEmpty() || mvSymbol.isEmpty()) {
                return querySymbol.isEmpty() && mvSymbol.isEmpty();
            }
            return mapsTo(querySymbol.get(), mvSymbol.get());
        }

        private boolean mapsTo(io.trino.sql.planner.Symbol querySymbol, Symbol mvSymbol)
        {
            return mvSymbol.equals(querySymbolToMvSymbolMapping.get(querySymbol));
        }

        /**
         * Records that {@code querySymbol} corresponds to {@code mvSymbol}. Observing a symbol again with the same
         * counterpart is a match; a different counterpart is not.
         */
        private boolean bind(io.trino.sql.planner.Symbol querySymbol, Symbol mvSymbol)
        {
            Symbol previous = querySymbolToMvSymbolMapping.putIfAbsent(querySymbol, mvSymbol);
            return previous == null || previous.equals(mvSymbol);
        }

        private Map<io.trino.sql.planner.Symbol, Symbol> buildQuerySymbolToMvSymbolMapping()
        {
            return ImmutableMap.copyOf(querySymbolToMvSymbolMapping);
        }

        private MaterializedViewSubstitutionOutcome outcome()
        {
            return outcome;
        }
    }

    /**
     * Memoizes {@link SubstitutionMetadata#getColumnId} for one optimizer pass: each lookup builds a connector session,
     * and the same columns are otherwise resolved again for every node and candidate. Not thread-safe; one per pass.
     */
    private static class QueryColumnIds
    {
        private record Key(TableHandle table, ColumnHandle column) {}

        private final SubstitutionMetadata substitutionMetadata;
        private final Session session;
        private final Map<Key, Optional<ConnectorColumnId>> columnIds = new HashMap<>();

        private QueryColumnIds(SubstitutionMetadata substitutionMetadata, Session session)
        {
            this.substitutionMetadata = requireNonNull(substitutionMetadata, "substitutionMetadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        private Optional<ConnectorColumnId> get(TableHandle table, ColumnHandle column)
        {
            return columnIds.computeIfAbsent(new Key(table, column), _ -> substitutionMetadata.getColumnId(session, table, column));
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
