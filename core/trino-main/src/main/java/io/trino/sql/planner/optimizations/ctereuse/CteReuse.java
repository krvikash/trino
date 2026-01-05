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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.UnificationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.ScalarProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.dialect.trino.operationmetadata.DynamicFilterSourceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.CheckpointReferences;
import io.trino.sql.planner.optimizations.ctereuse.Checkpoint.BottomCheckpoint;
import io.trino.sql.planner.optimizations.ctereuse.Checkpoint.IntermediateCheckpoint;
import io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.DynamicFilterExtractionResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isDebugCteReuseEnabled;
import static io.trino.plugin.base.expression.ConnectorExpressions.and;
import static io.trino.plugin.base.util.ConnectorExpressionUtil.extractVariableNames;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.dialect.trino.ProgramBuilder.initializeNameAllocator;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DISTRIBUTION_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.JOIN_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.MAY_SKIP_OUTPUT_DUPLICATES;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.SPILLABLE;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.STATISTICS_AND_COST_SUMMARY;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPassthroughMapping;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPrunedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getPruningAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getReorderingAssignments;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getSelectedFields;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.identityBranchToCheckpoint;
import static io.trino.sql.planner.optimizations.ctereuse.DeterminismUtils.isDeterministic;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.extractDynamicConjunct;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.extractDynamicFilters;
import static io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.isDynamicFilterFunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.disjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.hoistCommonConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.isTrue;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.optimizeLogicalOperations;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.removeConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.extractReferencedFields;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class CteReuse
{
    private CteReuse()
    {}

    public static Optional<Program> reuseCommonSubqueries(Plan plan, PlannerContext plannerContext, Session session, FormatOptions formatOptions)
    {
        boolean debugEnabled = isDebugCteReuseEnabled(session);
        Logger log = Logger.get(CteReuse.class);
        PrintOptions printOptions = formatOptions.printOptions();

        // rewrite to new IR
        Program program;
        try {
            program = ProgramBuilder.buildProgram(plan.getRoot());
        }
        catch (UnsupportedOperationException | TrinoException e) {
            if (debugEnabled) {
                log.info("Failed to translate the query plan to the new IR for query: " + session.getQueryId());
            }
            return Optional.empty();
        }

        // proceed only if this is a SELECT statement. This is true if all tables have updateTarget == false
        if (hasUpdateTarget(program)) {
            if (debugEnabled) {
                log.info("Cannot apply CTE reuse for query %s: it is an update query.\nQuery program: %s", session.getQueryId(), program.print(printOptions));
            }
            return Optional.empty();
        }

        Set<TableScan> topLevelTableScanOperations = getTopLevelTableScanOperations(program);

        // group TableScan operations by catalog
        Multimap<CatalogHandle, TableScan> tableScansByCatalog = Multimaps.index(topLevelTableScanOperations, tableScan -> TABLE_HANDLE.getAttribute(tableScan.attributes()).catalogHandle());

        // find compatible subgroups for each connector. singleton subgroups are excluded
        List<UnifiedGroup> unifiedGroups = tableScansByCatalog.asMap().values().stream()
                .flatMap(catalogTables -> unifyTableSubgroups(catalogTables, session, plannerContext.getMetadata()).stream())
                .collect(toImmutableList());

        if (unifiedGroups.isEmpty()) {
            if (debugEnabled) {
                log.info("CTE reuse is ineffective for query %s: no tables to unify found.\nQuery program: %s", session.getQueryId(), program.print(printOptions));
            }
            return Optional.empty();
        }

        // initialize the (operation -> downstream operations) map for the original program
        // it will be used to traverse the original program and will not be updated when we change parts of the program
        // TODO operations should be always compared scope-aware. Theoretically, there can be equal operations in different scopes of one plan. The keys and values of operationToDownstream should be (Operation, scope)
        // For now, it is an IdentityHashMap to avoid clashing operations from different scopes.
        Map<Operation, Operation> operationToDownstream = buildOperationToDownstream(program);

        // initialize a ValueNameAllocator compatible with the original program
        // when we create new values using this allocator, they will be ready to incorporate
        // in the original program without causing duplicate name issues
        ProgramBuilder.ValueNameAllocator nameAllocator = initializeNameAllocator(program);

        // initialize a global MultiGroupMerger to facilitate merging multi-source operations like Join or Exchange
        MultiGroupMerger multiGroupMerger = new MultiGroupMerger();

        // initialize a collection of all newly created operations
        Map<Value, Operation> newOperations = new HashMap<>();

        // merge each group
        for (UnifiedGroup unifiedGroup : unifiedGroups) {
            UnifiedStates initializedGroup = initializeTraversalForGroup(unifiedGroup, plannerContext.getMetadata(), operationToDownstream, nameAllocator, newOperations, plannerContext, session);
            mergeGroupRecursively(
                    initializedGroup,
                    ImmutableList.of(new BottomCheckpoint(unifiedGroup.tableScans())),
                    identityBranchToCheckpoint(unifiedGroup.tableScans().size()),
                    false,
                    operationToDownstream,
                    nameAllocator,
                    newOperations,
                    multiGroupMerger,
                    plannerContext,
                    session,
                    plannerContext.getMetadata());
        }

        // handle all leftover hanging groups in multiGroupMerger
        multiGroupMerger.flush(nameAllocator, newOperations);

        // create the new plan consisting of old and new operations
        Block oldMainBlock = ((Query) program.getRoot()).query();
        Block newMainBlock = layoutOperations(oldMainBlock, newOperations, false);

        // clean up dynamic filters
        newMainBlock = cleanUpDynamicFilters(newMainBlock, nameAllocator);

        Program newProgram = new Program(((Query) program.getRoot()).withRegions(ImmutableList.of(singleBlockRegion(newMainBlock))));

        if (debugEnabled) {
            log.info("CTE reuse applied for query %s.\nQuery program before: %s\n\nQuery program after: %s", session.getQueryId(), program.print(printOptions), newProgram.print(printOptions));
        }

        return Optional.of(newProgram);
    }

    private static boolean hasUpdateTarget(Program program)
    {
        Operation root = program.getRoot();
        return hasUpdateTarget(root);
    }

    private static boolean hasUpdateTarget(Operation operation)
    {
        if (operation instanceof TableScan tableScan && UPDATE_TARGET.getAttribute(tableScan.attributes())) {
            return true;
        }
        for (Region region : operation.regions()) {
            for (Block block : region.blocks()) {
                for (Operation nested : block.operations()) {
                    if (hasUpdateTarget(nested)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * We only search for TableScan operations on the top level of the program. We don't search recursively in nested Blocks.
     * <p>
     * Correlated subqueries are represented as nested Blocks. Currently, a valid optimized plan does not contain any correlation,
     * and Trino cannot execute correlated queries without de-correlating them first.
     * As soon as Trino can execute correlated queries, the correlated subqueries will be very good candidates for CTE reuse
     * because they are executed multiple times, once for each input row.
     */
    private static Set<TableScan> getTopLevelTableScanOperations(Program program)
    {
        Block topLevelBlock = ((Query) program.getRoot()).query();

        return topLevelBlock.operations().stream()
                .filter(TableScan.class::isInstance)
                .map(TableScan.class::cast)
                .collect(toImmutableSet());
    }

    /**
     * Given a collection of tables: [T1, T2, T3, T4, T5, T6], the algorithm identifies groups of compatible tables,
     * and unifies each group in a sequence of `unifyTables()` operations.
     * <p>
     * For example, if T1, T2, and T5 are compatible, they are unified in two steps:
     * - unifyTables(T1, T2) --> Result1(Unified1, Compensation1, Compensation2)
     * - unifyTables(Unified1, T5) --> Result2(Unified2, Compensation3, Compensation4)
     * <p>
     * Result2 is the final unification result for the whole group, and Unified2 is the unified table handle.
     * To restore T1 from unified2, we must compose Compensation3 with Compensation1.
     * To restore T2 from unified2, we must compose Compensation3 with Compensation2.
     * To restore T5 from unified2, we must apply Compensation4.
     * <p>
     * After each successful `unifyTables()` call, the unification result is recorded
     * under the second one of the unified tables:
     * - Result1 is recorded under "2",
     * - Result2 is recorded under "5".
     * <p>
     * The final unification result, recorded under "5", has the unified table handle
     * for the whole group.
     * To get the right compensations for all tables in the group, we must visit
     * the final unification result as well as the intermediate results.
     * <p>
     * Note: the first table of a group has no unification result recorded.
     * It also applies to singleton groups, but those are filtered out from the result.
     * <p>
     * Note: generally, TableScan operations may have duplicates in their ColumnHandle list,
     * which means that one column handle backs multiple output fields. We exclude those TableScans.
     * This is for the purpose of having a clear 1-1 correspondence between the columns of the unified TableScan
     * and the column of the component TableScans. It is important when handling the pruning projections.
     */
    public static List<UnifiedGroup> unifyTableSubgroups(Collection<TableScan> tableScans, Session session, Metadata metadata)
    {
        // exclude tables that have duplicates in the column handles list
        List<TableScan> tables = ImmutableList.copyOf(tableScans).stream()
                .filter(table -> {
                    List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(table.attributes());
                    return columnHandles.stream().distinct().count() == columnHandles.size();
                })
                .collect(toImmutableList());

        // group id is the index of the first table in the group
        int[] groupIds = new int[tables.size()];
        Arrays.fill(groupIds, -1);
        // attach the unification result to the second one of the unified tables
        ImmutableMap.Builder<Integer, UnificationResult<TableHandle>> unificationResultsBuilder = ImmutableMap.builder();

        for (int i = 0; i < tables.size(); i++) {
            if (groupIds[i] == -1) {
                // this table does not belong to any group yet, start a new group
                groupIds[i] = i;
                TableHandle first = TABLE_HANDLE.getAttribute(tables.get(i).attributes());
                for (int j = i + 1; j < tables.size(); j++) {
                    if (groupIds[j] == -1 && Objects.equals(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(tables.get(i).attributes()), USE_CONNECTOR_NODE_PARTITIONING.getAttribute(tables.get(j).attributes()))) {
                        TableHandle second = TABLE_HANDLE.getAttribute(tables.get(j).attributes());
                        Optional<UnificationResult<TableHandle>> result = metadata.unifyTables(session, first, second);
                        if (result.isPresent()) {
                            // add the table to the group
                            groupIds[j] = i;
                            first = result.orElseThrow().unifiedHandle();
                            unificationResultsBuilder.put(j, result.orElseThrow());
                        }
                    }
                }
            }
        }
        Map<Integer, UnificationResult<TableHandle>> unificationResults = unificationResultsBuilder.buildOrThrow();

        ImmutableListMultimap.Builder<Integer, Integer> groupIndexesBuilder = ImmutableListMultimap.builder();
        for (int i = 0; i < groupIds.length; i++) {
            groupIndexesBuilder.put(groupIds[i], i);
        }

        return groupIndexesBuilder.build().asMap().values().stream()
                .filter(indexCollection -> indexCollection.size() > 1)
                .map(indexCollection -> {
                    List<Integer> indexes = ImmutableList.copyOf(indexCollection);
                    return new UnifiedGroup(
                            indexes.stream()
                                    .map(tables::get)
                                    .collect(toImmutableList()),
                            indexes.subList(1, indexes.size()).stream()
                                    .map(unificationResults::get)
                                    .collect(toImmutableList()));
                })
                .collect(toImmutableList());
    }

    /**
     * Map each operation to the operation that uses this operation's result as an argument.
     * Use mapping by identity to avoid clashing operations from different scopes.
     */
    private static Map<Operation, Operation> buildOperationToDownstream(Program program)
    {
        Map<Operation, Operation> operationToDownstream = new IdentityHashMap<>();
        Block mainBlock = ((Query) program.getRoot()).query();
        buildOperationToDownstream(mainBlock, AccessibleValueMap.initialize(), operationToDownstream);
        return operationToDownstream;
    }

    private static void buildOperationToDownstream(Block block, AccessibleValueMap outerScope, Map<Operation, Operation> operationToDownstream)
    {
        AccessibleValueMap currentScope = outerScope.forNestedBlock(block);

        for (Operation operation : block.operations()) {
            // record uses of operation results in this operation's arguments
            for (Value argument : operation.arguments()) {
                SourceNode source = currentScope.getSource(argument);
                if (source instanceof Operation sourceOperation) {
                    if (operationToDownstream.put(sourceOperation, operation) != null) {
                        throw new TrinoException(IR_ERROR, format("Operation result %s is used by multiple operations", sourceOperation.result().name()));
                    }
                }
            }
            // visit nested blocks
            for (Region region : operation.regions()) {
                Block nestedBlock = region.getOnlyBlock();
                buildOperationToDownstream(nestedBlock, currentScope, operationToDownstream);
            }
            // add operation result to scope
            currentScope = currentScope.withOperationResult(operation);
        }
    }

    /**
     * Initialize traversal for a group of compatible table scans.
     */
    public static UnifiedStates initializeTraversalForGroup(
            UnifiedGroup unifiedGroup,
            Metadata metadata,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            PlannerContext plannerContext,
            Session session)
    {
        // unified TableScan is the first unified operation
        TableScan unifiedTableScan = getUnifiedTableScan(unifiedGroup, nameAllocator);
        newOperations.put(unifiedTableScan.result(), unifiedTableScan);

        // initialize traversal context for each branch on top of the unified TableScan
        List<TraversalContext> traversalContexts = initializeTraversalContexts(unifiedGroup, unifiedTableScan, metadata, nameAllocator, plannerContext, session);

        // initialize traversal state for each branch by combining the TraversalContext with the next downstream operation
        ImmutableList.Builder<TraversalState> traversalStates = ImmutableList.builder();
        for (int i = 0; i < unifiedGroup.tableScans().size(); i++) {
            traversalStates.add(new TraversalState(traversalContexts.get(i), getNextOperation(unifiedGroup.tableScans().get(i), operationToDownstream)));
        }

        return new UnifiedStates(unifiedTableScan, traversalStates.build());
    }

    /**
     * Create the unified TableScan operation that will replace the TableScan operations of all the unified branches.
     * <p>
     * Exposed columns:
     * The unified TableScan must expose all columns referenced by either of the component tables.
     * Additionally, it must expose all columns used by the compensation predicates of all the component tables.
     * It is not guaranteed that the compensation predicates use only the referenced columns.
     * Example:
     * Table T1 [a, b, c]
     * -- a predicate (c > 0) is pushed down --> Table T1 [a, b, c] enforcedPredicate = (c > 0)
     * -- a pruning projection for column c is pushed down --> Table T1 [a, b] enforcedPredicate = (c > 0)
     * Table T2 [d, e] enforcedPredicate = (d < 0)
     * unifyTables(T1, T2) --> Table T3 enforcedPredicate = (c > 0 OR d < 0); compensation1 = (c > 0); compensation2 = (d < 0)
     * In order to restore semantics of T1, we must be able to apply compensation1 = (c > 0) on top of unified table T3.
     * For that purpose, we must expose column c, even though it was pruned from Table T1.
     * <p>
     * Note: we do not guarantee to expose all columns used by the enforced predicates.
     */
    private static TableScan getUnifiedTableScan(UnifiedGroup unifiedGroup, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // compute a set of referenced column handles from all component tables
        // and determine the output field type for each column handle
        Map<ColumnHandle, Type> typesMap = new LinkedHashMap<>();
        for (TableScan tableScan : unifiedGroup.tableScans()) {
            Type outputRowType = relationRowType(trinoType(tableScan.result().type()));
            if (outputRowType instanceof RowType rowType) {
                List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(tableScan.attributes());
                for (int i = 0; i < rowType.getTypeParameters().size(); i++) {
                    addType(typesMap, columnHandles.get(i), rowType.getTypeParameters().get(i));
                }
            }
        }
        // compute a set of columns used in compensation predicates
        unifiedGroup.unificationResults().stream()
                .flatMap(unificationResult -> Stream.of(
                        unificationResult.firstCompensationFilter(),
                        unificationResult.secondCompensationFilter()))
                .filter(tupleDomain -> !tupleDomain.isNone())
                .flatMap(tupleDomain -> tupleDomain.getDomains().orElseThrow().entrySet().stream())
                .forEach(entry -> addType(typesMap, entry.getKey(), entry.getValue().getType()));

        // compute a set of columns used in compensation expressions
        for (UnificationResult<TableHandle> unificationResult : unifiedGroup.unificationResults()) {
            addExpressionTypes(typesMap, unificationResult.firstCompensationExpression(), unificationResult.firstAssignments());
            addExpressionTypes(typesMap, unificationResult.secondCompensationExpression(), unificationResult.secondAssignments());
        }

        List<Map.Entry<ColumnHandle, Type>> columnsList = typesMap.entrySet().stream().collect(toImmutableList());

        UnificationResult<TableHandle> unificationResult = unifiedGroup.unificationResults().getLast();

        List<ColumnHandle> unifiedColumnHandles = columnsList.stream()
                .map(Map.Entry::getKey)
                .collect(toImmutableList());
        Set<ColumnHandle> unifiedColumnHandlesSet = ImmutableSet.copyOf(unifiedColumnHandles);

        return new TableScan(
                nameAllocator.newName(),
                columnsList.isEmpty() ? EMPTY_ROW : RowType.anonymous(columnsList.stream()
                        .map(Map.Entry::getValue)
                        .collect(toImmutableList())),
                unificationResult.unifiedHandle(),
                unifiedColumnHandles,
                // the enforcedConstraint must be based on columns exposed by the TableScan
                unificationResult.enforcedProperties().tupleDomainConstraint()
                        .filter((columnHandle, domain) -> unifiedColumnHandlesSet.contains(columnHandle)),
                // TODO use the method deriveTableStatisticsForPushdown() to get the statistics for the unified TableScan
                Optional.empty(),
                false,
                Optional.ofNullable(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(unifiedGroup.tableScans().getFirst().attributes())));
    }

    private static void addExpressionTypes(Map<ColumnHandle, Type> typesMap, ConnectorExpression compensationExpression, Map<String, Assignment> assignments)
    {
        extractVariableNames(compensationExpression).stream()
                .map(assignments::get)
                .forEach(assignment -> addType(typesMap, assignment.getColumn(), assignment.getType()));
    }

    private static void addType(Map<ColumnHandle, Type> typesMap, ColumnHandle columnHandle, Type type)
    {
        Type previous = typesMap.put(columnHandle, type);
        if (previous != null && !type.equals(previous)) {
            throw new TrinoException(IR_ERROR, format("different types: %s and %s for the same column handle: %s", previous.getDisplayName(), type.getDisplayName(), columnHandle));
        }
    }

    /**
     * Initialize traversal contexts for a group of branches after their initial TableScan operations were unified.
     * <p>
     * In each of the branches, the initial TableScan operation will be replaced with the unified TableScan.
     * For each branch, the TraversalContext carries whatever differences there are between the original TableScan
     * and the unified TableScan, so that we can restore the original semantics later.
     * Additionally, the TraversalContext carries the properties of the unified TableScan. It will help us to avoid
     * repetition when we apply filter or limit operations.
     */
    private static List<TraversalContext> initializeTraversalContexts(UnifiedGroup unifiedGroup, TableScan unifiedTableScan, Metadata metadata, ProgramBuilder.ValueNameAllocator nameAllocator, PlannerContext plannerContext, Session session)
    {
        ImmutableList.Builder<TraversalContext> resultBuilder = ImmutableList.builder();

        // the enforced properties of the unified table scan are common to all the unified branches. Get it from the final unification result.
        UnificationResult.Properties enforcedProperties = unifiedGroup.unificationResults().getLast().enforcedProperties();
        // prune the parts of enforced filter which are not supported by the exposed columns
        Set<ColumnHandle> unifiedHandlesSet = ImmutableSet.copyOf(COLUMN_HANDLES.getAttribute(unifiedTableScan.attributes()));
        TupleDomain<ColumnHandle> prunedEnforcedTupleDomain = enforcedProperties.tupleDomainConstraint()
                .filter((columnHandle, domain) -> unifiedHandlesSet.contains(columnHandle));

        List<ConnectorExpression> conjuncts = ConnectorExpressions.extractConjuncts(enforcedProperties.connectorExpressionConstraint()).stream()
                .filter(conjunct -> extractVariableNames(conjunct).stream()
                        .allMatch(variableName -> unifiedHandlesSet.contains(enforcedProperties.connectorExpressionAssignments().get(variableName).getColumn())))
                .toList();
        List<ExpressionAndAssignments> enforcedExpressionAndAssignments = List.of(new ExpressionAndAssignments(and(conjuncts), toColumnHandleMap(enforcedProperties.connectorExpressionAssignments())));
        Block enforcedPredicate = translateToBlock(unifiedTableScan, prunedEnforcedTupleDomain, enforcedExpressionAndAssignments, plannerContext, session, metadata, nameAllocator);
        OptionalLong enforcedLimit = enforcedProperties.limit();

        // map ColumnHandles to field indexes in the unifiedTableScan
        ImmutableMap.Builder<ColumnHandle, Integer> unifiedIndexesBuilder = ImmutableMap.builder();
        List<ColumnHandle> unifiedHandles = COLUMN_HANDLES.getAttribute(unifiedTableScan.attributes());
        for (int i = 0; i < unifiedHandles.size(); i++) {
            // the unified TableScan has no duplicate ColumnHandles, as they were de-duplicated in the `getUnifiedTableScan()` method
            unifiedIndexesBuilder.put(unifiedHandles.get(i), i);
        }
        Map<ColumnHandle, Integer> unifiedIndexes = unifiedIndexesBuilder.buildOrThrow();

        // process component tables in reverse order to compose compensations
        TupleDomain<ColumnHandle> currentCompensationPredicate = TupleDomain.all();
        List<ExpressionAndAssignments> currentCompensationConjuncts = new ArrayList<>();
        for (int i = unifiedGroup.tableScans().size() - 1; i >= 1; i--) {
            TableScan tableScan = unifiedGroup.tableScans().get(i);
            UnificationResult<TableHandle> unificationResult = unifiedGroup.unificationResults().get(i - 1);
            FieldMapping fieldMapping = computeMapping(tableScan, unifiedIndexes);
            Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
            TupleDomain<ColumnHandle> tupleDomain = currentCompensationPredicate.intersect(unificationResult.secondCompensationFilter());
            List<ExpressionAndAssignments> expressionAndAssignments = new ArrayList<>(currentCompensationConjuncts);
            expressionAndAssignments.add(new ExpressionAndAssignments(unificationResult.secondCompensationExpression(), toColumnHandleMap(unificationResult.secondAssignments())));
            Block compensationPredicate = translateToBlock(unifiedTableScan, tupleDomain, expressionAndAssignments, plannerContext, session, metadata, nameAllocator);
            currentCompensationPredicate = currentCompensationPredicate.intersect(unificationResult.firstCompensationFilter());
            currentCompensationConjuncts.add(new ExpressionAndAssignments(unificationResult.firstCompensationExpression(), toColumnHandleMap(unificationResult.firstAssignments())));
            resultBuilder.add(new TraversalContext(fieldMapping, fieldsToPrune, compensationPredicate, enforcedPredicate, enforcedLimit));
        }

        // process the first component table
        TableScan tableScan = unifiedGroup.tableScans().get(0);
        FieldMapping fieldMapping = computeMapping(tableScan, unifiedIndexes);
        Set<Integer> fieldsToPrune = computeFieldsToPrune(tableScan, unifiedIndexes);
        Block compensationPredicate = translateToBlock(unifiedTableScan, currentCompensationPredicate, currentCompensationConjuncts, plannerContext, session, metadata, nameAllocator);
        resultBuilder.add(new TraversalContext(fieldMapping, fieldsToPrune, compensationPredicate, enforcedPredicate, enforcedLimit));

        return resultBuilder.build().reverse();
    }

    private static Map<String, ColumnHandle> toColumnHandleMap(Map<String, Assignment> assignments)
    {
        return assignments.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getColumn()));
    }

    /**
     * Translate TupleDomain and {@code List<ConnectorExpression>} to Block based on the output type of the provided TableScan.
     * <p>
     * This method uses a temporary hack. It invokes DomainTranslator and ConnectorExpressionTranslator
     * to translate the TupleDomain and the {@code List<ConnectorExpression>} to the old IR: TupleDomain -> Expression and each ConnectorExpression -> Expression.
     * Then we translate from the old IR to the new IR: Expression -> Block. We should translate directly: TupleDomain -> Block and ConnectorExpression -> Block.
     * However, the domain translation is not trivial, and it involves optimization of the created predicate. It will not be migrated to the new IR
     * as part of the CTE reuse POC.
     * TODO rewrite DomainTranslator and ConnectorExpressionTranslator to new IR
     */
    private static Block translateToBlock(
            TableScan tableScan,
            TupleDomain<ColumnHandle> tupleDomain,
            List<ExpressionAndAssignments> expressionAndAssignmentsList,
            PlannerContext plannerContext,
            Session session,
            Metadata metadata,
            ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        List<ColumnHandle> columnHandles = COLUMN_HANDLES.getAttribute(tableScan.attributes());
        Type relationRowType = relationRowType(trinoType(tableScan.result().type()));
        ImmutableMap.Builder<ColumnHandle, Symbol> columnMapBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> symbolListBuilder = ImmutableList.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            Symbol symbol = symbolAllocator.newSymbol("tmp_hack", relationRowType.getTypeParameters().get(i));
            columnMapBuilder.put(columnHandles.get(i), symbol);
            symbolListBuilder.add(symbol);
        }
        Map<ColumnHandle, Symbol> columnMap = columnMapBuilder.buildOrThrow();
        List<Symbol> symbolList = symbolListBuilder.build();

        List<Expression> expressionConjuncts = new ArrayList<>(expressionAndAssignmentsList.size() + 1);
        for (ExpressionAndAssignments expressionAndAssignments : expressionAndAssignmentsList) {
            Map<String, Symbol> variableMappings = expressionAndAssignments.assignments().entrySet().stream()
                    .filter(entry -> columnMap.containsKey(entry.getValue()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> columnMap.get(entry.getValue())));

            expressionConjuncts.add(ConnectorExpressionTranslator.translate(session, expressionAndAssignments.expression(), plannerContext, variableMappings));
        }
        TupleDomain<Symbol> symbolTupleDomain = tupleDomain.transformKeys(columnMap::get);
        expressionConjuncts.add(new DomainTranslator(metadata).toPredicate(symbolTupleDomain));
        Expression expression = IrUtils.combineConjuncts(expressionConjuncts);

        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(relationRowType));
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(parameter));
        ImmutableMap.Builder<Symbol, Context.RowField> symbolMapping = ImmutableMap.builder();
        for (int i = 0; i < symbolList.size(); i++) {
            symbolMapping.put(symbolList.get(i), new Context.RowField(parameter, i));
        }
        // rewrite the Expression to block
        expression.accept(
                new ScalarProgramBuilder(nameAllocator),
                new Context(blockBuilder, symbolMapping.buildOrThrow()));
        // add Return operation to finish the Block
        // TODO extract and reuse addReturnOperation() from RelationalProgramBuilder
        Operation recentOperation = blockBuilder.recentOperation();
        Return returnOperation = new Return(nameAllocator.newName(), recentOperation.result(), recentOperation.attributes());
        blockBuilder.addOperation(returnOperation);
        return optimizeLogicalOperations(blockBuilder.build());
    }

    /**
     * Compute mapping to rewrite references to the output fields of componentTableScan in terms of the output fields of unifiedTableScan.
     */
    private static FieldMapping computeMapping(TableScan componentTableScan, Map<ColumnHandle, Integer> unifiedIndexes)
    {
        List<ColumnHandle> componentHandles = COLUMN_HANDLES.getAttribute(componentTableScan.attributes());
        if (componentHandles.isEmpty()) {
            return FieldMapping.EMPTY;
        }

        ImmutableMap.Builder<Integer, Integer> indexMapping = ImmutableMap.builder();
        for (int i = 0; i < componentHandles.size(); i++) {
            indexMapping.put(i, unifiedIndexes.get(componentHandles.get(i)));
        }

        return new FieldMapping(indexMapping.buildOrThrow());
    }

    /**
     * Find which fields of the unifiedTableScan were not originally present in the componentTableScan,
     * and should be pruned when we want to restore the componentTableScan semantics.
     * <p>
     * Note: for the componentTableScan, there might be extracted predicate, recorded as TraversalContext.predicateToApply,
     * which uses some fields identified as fields to prune. We won't be able to effectively prune those fields
     * until that predicate is satisfied.
     */
    private static Set<Integer> computeFieldsToPrune(TableScan componentTableScan, Map<ColumnHandle, Integer> unifiedIndexes)
    {
        Set<ColumnHandle> componentHandles = ImmutableSet.copyOf(COLUMN_HANDLES.getAttribute(componentTableScan.attributes()));

        return unifiedIndexes.entrySet().stream()
                .filter(entry -> !componentHandles.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableSet());
    }

    /**
     * Return the _only_ downstream operation of the provided operation. It is the operation that uses this operation's result as an argument.
     * The sourceIndex indicates which argument of the downstream operation the current operation is.
     * <p>
     * This method should fail if the provided operation's result is not used. This is the case for terminal operations, like Output, or Return, or for dead code.
     * For now, this is not achievable: we will not call this method on terminal operations, and there shall be no dead code.
     */
    public static OperationAndIndex getNextOperation(Operation operation, Map<Operation, Operation> operationToDownstream)
    {
        Operation nextOperation = requireNonNull(operationToDownstream.get(operation), format("Operation result %s is not used in the program", operation.result().name()));
        return new OperationAndIndex(nextOperation, nextOperation.arguments().indexOf(operation.result()));
    }

    /**
     * The next downstream operation and the index of the upstream operation as the downstream operation's source.
     */
    public record OperationAndIndex(Operation operation, int sourceIndex)
    {
        public OperationAndIndex
        {
            requireNonNull(operation, "operation is null");
        }
    }

    /**
     * Merge branches on top of the unifiedOperation.
     * <p>
     * Notes about creating new operations.
     * The algorithm creates new relational operations for different purposes:
     * - to represent common part of semantics of the merged branches
     * - to merge operations from different branches
     * - to compensate for differences after merging branches
     * - to wire the rewritten part of the plan to the original plan
     * Generally, each created operation has a new result name obtained from the ValueNameAllocator so that there
     * cannot be a clash between the original operations and the new operations.
     * One exception to this rule is the situation when we wire the old and new parts of the plan:
     * The border operation reuses the original result name so that it fits in with the original downstream plan.
     * When we create the new plan composed of the old and new operations, we always prioritize the new operations
     * so that we use the new border operation instead of the old operation.
     * <p>
     * Not all operations collected in newOperations are guaranteed to be used in the resulting plan.
     * Some of them might become redundant when the algorithm backtracks. This redundancy is not an issue.
     * The resulting plan will be created in the upstream direction, starting from the root (Output) operation.
     * Only those new operations which are achievable through the border operations will be included.
     * The algorithm never backtracks from a path where it created a border operation.
     * <p>
     * Notes on checkpoints.
     * Checkpoints are the sites where the algorithm backtracks when it can no longer merge the current group.
     * They are of two kinds:
     * - bottom: the algorithm backtracks down to table scans
     * - intermediate: the algorithm backtracks to a recent remote exchange
     * <p>
     * We try to avoid setting an intermediate checkpoint when the merged branches didn't do any meaningful work yet
     * -- for example when they only did table scans. In such case we prefer to backtrack down to table scans.
     * We use the setCheckpoint property to decide whether the next remote exchange should be used as an intermediate checkpoint.
     * <p>
     * Initially, there is a single checkpoint for a group, and each branch in the group has a corresponding checkpoint entry.
     * Things become more complicated when we merge a multi-source operation, like Join or Union.
     * When we backtrack, we might have to descend into each source individually to the recent checkpoint.
     * To enable that, we model checkpoint as a list of Checkpoint objects and a mapping for each branch of the current group
     * that points to the appropriate branches in each checkpoint.
     * Example:
     * Group A has 8 branches pointing to 4 Union operations
     * a0, a1 -> u0
     * a2, a3 -> u1
     * a4, a5 -> u2
     * a6, a7 -> u3
     * Checkpoint for group A has 8 branches: c0, c1, c2, c3, c4, c5, c6, c7
     * Each of the Union operations has another source. All the missing sources are found in another group B:
     * b0 -> u3
     * b1 -> u2
     * b2 -> u1
     * b3 -> u0
     * Checkpoint for group B has 4 branches: d0, d1, d2, d3
     * Now the Union operations can be fully merged (it involves the data duplicating operations for branches from group A)
     * The merged result X has 4 branches
     * u0 -> next0
     * u1 -> next1
     * u2 -> next2
     * u3 -> next3
     * What is the checkpoint for group X? Each branch in X is a result of 3 source branches: 2 branches from A, and 1 branch from B.
     * For branch u0 -> next0, the checkpoint is [c0, c1, d3]
     * For branch u1 -> next1, the checkpoint is [c2, c3, d2], and so on.
     * If the group X is further composed, each resulting branch will have even more checkpoints.
     * Eventually, a remote exchange will establish a new checkpoint with simple 1-1 correspondence of branches.
     *
     * @param unifiedStates -- the recent unified operation and a list of TraversalStates to be merged. All branches are based on the unified operation. The order of branches is meaningful.
     * @param checkpoints -- the points to backtrack to when merging fails. Current branches correspond to the checkpoint branches as in the branchToCheckpoint mapping.
     * @param branchToCheckpoint -- the mapping of current branches to the corresponding branches in checkpoints.
     * @param setCheckpoint -- indicates whether the next remote exchange should be used as a checkpoint
     * @param operationToDownstream -- map (operation -> downstream operation) in the original plan
     * @param nameAllocator -- ValueNameAllocator needed for creating new operations
     * @param newOperations -- a collection of newly created operations
     * @param multiGroupMerger -- a structure to enable merging multi-source operations, like Join. It records operations whose sources
     * belong to multiple groups.
     */
    public static void mergeGroupRecursively(
            UnifiedStates unifiedStates,
            List<Checkpoint> checkpoints,
            BranchesToCheckpointsMapping branchToCheckpoint,
            boolean setCheckpoint,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            MultiGroupMerger multiGroupMerger,
            PlannerContext plannerContext,
            Session session,
            Metadata metadata)
    {
        Operation unifiedOperation = unifiedStates.unifiedOperation();
        List<TraversalState> branches = unifiedStates.residualStates();

        checkArgument(branches.size() >= 2, "attempt to merge less than two branches");
        branchToCheckpoint.verifyMapping(branches.size(), checkpoints);

        // for each branch, ingest operations into context as long as possible
        List<TraversalState> ingestedStates = branches.stream()
                .map(branch -> ingestOperationsIntoContext(branch, unifiedOperation, operationToDownstream, nameAllocator))
                .collect(toImmutableList());

        // each branch is fully ingested. extract the common part of all branches, and add operations for it
        UnifiedStatesAndCheckpointRequirement commonPartMergedAndCheckpointRequirement = outputCommonSemantics(ingestedStates, unifiedOperation, nameAllocator, newOperations);
        UnifiedStates commonPartMerged = commonPartMergedAndCheckpointRequirement.unifiedStates();
        setCheckpoint |= commonPartMergedAndCheckpointRequirement.requireCheckpoint();

        // analyze the next operations for all branches and find subgroups that can be merged
        Subgroups subgroups = identifySubgroupsToMerge(commonPartMerged, operationToDownstream, nameAllocator, newOperations, multiGroupMerger, plannerContext, session, metadata);
        verifySubgroups(subgroups, commonPartMerged.residualStates().size());

        // case 1: all branches belong to one single-group merge or to one multi-group merge. Merge and proceed.
        if (subgroups.singleGroupMerges().size() + subgroups.multiGroupMerges().size() == 1 && subgroups.hangingBranches().isEmpty() && subgroups.remainingSingleGroupBranches().isEmpty()) {
            UnifiedStatesAndCheckpointMapping nextOperationMerged;
            if (subgroups.singleGroupMerges().size() == 1) {
                nextOperationMerged = SingleGroupMerger.mergeNextSingleGroupOperation(commonPartMerged.unifiedOperation(), commonPartMerged.residualStates(), checkpoints, branchToCheckpoint, operationToDownstream, nameAllocator, newOperations);
            }
            else {
                setCheckpoint |= multiGroupMerger.isCheckpointRequired(getOnlyElement(subgroups.multiGroupMerges()).hangingGroups());
                nextOperationMerged = multiGroupMerger.mergeNextMultiGroupOperation(commonPartMerged.unifiedOperation(), commonPartMerged.residualStates(), checkpoints, branchToCheckpoint, getOnlyElement(subgroups.multiGroupMerges()).hangingGroups(), operationToDownstream, nameAllocator, newOperations);
            }
            // the merged result might be a singleton branch, for example after merging a single-group self-Join. In such case, compensate and wire
            if (nextOperationMerged.unifiedStates().residualStates().size() == 1) {
                compensateAndWire(getOnlyElement(nextOperationMerged.unifiedStates().residualStates()), nextOperationMerged.unifiedStates().unifiedOperation(), nameAllocator, newOperations);
            }
            else {
                List<Checkpoint> newCheckpoints = nextOperationMerged.checkpoints();
                BranchesToCheckpointsMapping newBranchToCheckpoint = nextOperationMerged.branchToCheckpoint();
                // if the merged operation is a remote exchange, it can be used as a checkpoint
                if (setCheckpoint && nextOperationMerged.unifiedStates().unifiedOperation() instanceof Exchange exchange && EXCHANGE_SCOPE.getAttribute(exchange.attributes()).equals(REMOTE)) {
                    newCheckpoints = ImmutableList.of(new IntermediateCheckpoint(nextOperationMerged.unifiedStates()));
                    newBranchToCheckpoint = identityBranchToCheckpoint(nextOperationMerged.unifiedStates().residualStates().size());
                }
                setCheckpoint |= setCheckpointAfter(nextOperationMerged.unifiedStates().unifiedOperation());
                mergeGroupRecursively(
                        nextOperationMerged.unifiedStates(),
                        newCheckpoints,
                        newBranchToCheckpoint,
                        setCheckpoint,
                        operationToDownstream,
                        nameAllocator,
                        newOperations,
                        multiGroupMerger,
                        plannerContext,
                        session,
                        metadata);
            }
        }
        // case 2: all branches are "hanging". register the hanging group for future reference
        else if (subgroups.singleGroupMerges().isEmpty() && subgroups.multiGroupMerges().isEmpty() && subgroups.remainingSingleGroupBranches().isEmpty()) {
            multiGroupMerger.registerHangingGroup(commonPartMerged.unifiedOperation(), commonPartMerged.residualStates(), checkpoints, branchToCheckpoint, setCheckpoint);
        }
        // case 3: split merging into subgroups and backtrack
        else {
            // collect all subgroups
            ImmutableList.Builder<List<Integer>> allSubgroups = ImmutableList.builder();
            allSubgroups.addAll(subgroups.singleGroupMerges());
            subgroups.multiGroupMerges().stream()
                    .map(MultiGroupMerger.MultiGroupMerge::newGroupBranches)
                    .forEach(allSubgroups::add);
            // extract the "hanging branches" as another subgroup
            if (subgroups.hangingBranches().size() > 1) {
                allSubgroups.add(subgroups.hangingBranches());
            }
            // for all subgroups, select and merge recursively subgroups from all corresponding checkpoints
            for (List<Integer> subgroupIndexes : allSubgroups.build()) {
                for (int i = 0; i < checkpoints.size(); i++) {
                    Checkpoint checkpoint = checkpoints.get(i);
                    ImmutableList.Builder<Integer> checkpointReferences = ImmutableList.builder();
                    for (int branch : subgroupIndexes) {
                        checkpointReferences.addAll(branchToCheckpoint.getMappingForBranch(branch).getReferencesForCheckpoint(i));
                    }
                    UnifiedStates backtrackSubgroup = checkpoint.extractSubgroup(checkpointReferences.build(), operationToDownstream, nameAllocator, newOperations, plannerContext, session, metadata);
                    Checkpoint backtrackCheckpoint = checkpoint.extractSubgroupCheckpoint(checkpointReferences.build());
                    mergeGroupRecursively(
                            backtrackSubgroup,
                            ImmutableList.of(backtrackCheckpoint),
                            identityBranchToCheckpoint(backtrackCheckpoint.branchesCount()),
                            checkpoint instanceof IntermediateCheckpoint,
                            operationToDownstream,
                            nameAllocator,
                            newOperations,
                            multiGroupMerger,
                            plannerContext,
                            session,
                            metadata);
                }
            }
            // for all singleton branches, go back to all corresponding checkpoints, and compensate and wire all the corresponding branches
            ImmutableList.Builder<Integer> allSingletonBranches = ImmutableList.builder();
            allSingletonBranches.addAll(subgroups.remainingSingleGroupBranches());
            if (subgroups.hangingBranches().size() == 1) {
                allSingletonBranches.add(getOnlyElement(subgroups.hangingBranches()));
            }
            for (int branch : allSingletonBranches.build()) {
                CheckpointReferences checkpointReferences = branchToCheckpoint.getMappingForBranch(branch);
                for (int i = 0; i < checkpoints.size(); i++) {
                    // only compensate and wire branches from intermediate checkpoints. Skip the bottom branches -- the algorithm will find them when building the final plan.
                    if (checkpoints.get(i) instanceof IntermediateCheckpoint(UnifiedStates checkpointUnifiedStates)) {
                        List<Integer> references = checkpointReferences.getReferencesForCheckpoint(i);
                        for (int reference : references) {
                            compensateAndWire(checkpointUnifiedStates.residualStates().get(reference), checkpointUnifiedStates.unifiedOperation(), nameAllocator, newOperations);
                        }
                    }
                }
            }
        }
    }

    /**
     * Accumulate pruning projections and filters in the TraversalContext.
     */
    private static TraversalState ingestOperationsIntoContext(TraversalState branchState, Operation unifiedOperation, Map<Operation, Operation> operationToDownstream, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Operation nextOperation = branchState.nextOperation().operation();
        if (nextOperation instanceof Project project && project.isPruning() && isDeterministic(project)) {
            Block rebasedAssignments = rebaseBlock(project.assignments(), relationRowType(trinoType(unifiedOperation.result().type())), branchState.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            TraversalContext newContext = new TraversalContext(
                    getPassthroughMapping(rebasedAssignments).inverse(),
                    getPrunedFields(rebasedAssignments),
                    branchState.traversalContext().predicateToApply(),
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(project, operationToDownstream)), unifiedOperation, operationToDownstream, nameAllocator);
        }
        else if (nextOperation instanceof Filter filter && isDeterministic(filter)) {
            Block newPredicateToApply = rebaseBlock(filter.predicate(), relationRowType(trinoType(unifiedOperation.result().type())), branchState.traversalContext().fieldMapping(), nameAllocator).orElseThrow();
            newPredicateToApply = conjunction(ImmutableList.of(branchState.traversalContext().predicateToApply(), newPredicateToApply), nameAllocator);
            newPredicateToApply = optimizeLogicalOperations(newPredicateToApply);
            newPredicateToApply = removeConjuncts(newPredicateToApply, branchState.traversalContext().enforcedPredicate(), nameAllocator);
            TraversalContext newContext = new TraversalContext(
                    branchState.traversalContext().fieldMapping(),
                    branchState.traversalContext().fieldsToPrune(),
                    newPredicateToApply,
                    branchState.traversalContext().enforcedPredicate(),
                    branchState.traversalContext().enforcedLimit());
            return ingestOperationsIntoContext(new TraversalState(newContext, getNextOperation(filter, operationToDownstream)), unifiedOperation, operationToDownstream, nameAllocator);
        }
        return branchState;
    }

    /**
     * Compute the common part of the TraversalContexts, and the residual part of each context. Create operations for the unified part.
     * This method assumes that all provided states are based on baseUnifiedOperation, so they can be safely combined.
     * There must be at least two states provided.
     */
    private static UnifiedStatesAndCheckpointRequirement outputCommonSemantics(List<TraversalState> states, Operation baseUnifiedOperation, ProgramBuilder.ValueNameAllocator nameAllocator, Map<Value, Operation> newOperations)
    {
        checkArgument(states.size() > 1, "at least two branches must be provided for unification");

        List<TraversalContext> contexts = states.stream()
                .map(TraversalState::traversalContext)
                .collect(toImmutableList());

        // all contexts have the same enforced predicate
        Block enforcedPredicate = contexts.getFirst().enforcedPredicate();

        // separate dynamic filters and static filters from all branches
        List<DynamicFilterExtractionResult> extractionResults = contexts.stream()
                .map(TraversalContext::predicateToApply)
                .map(predicateToApply -> extractDynamicFilters(predicateToApply, nameAllocator))
                .collect(toImmutableList());
        List<Block> dynamicPredicatesToApply = extractionResults.stream()
                .map(DynamicFilterExtractionResult::dynamicPredicate)
                .collect(toImmutableList());
        List<Block> staticPredicatesToApply = extractionResults.stream()
                .map(DynamicFilterExtractionResult::staticPredicate)
                .collect(toImmutableList());

        // build the unified predicate so that all dynamic filters are collected in one conjunct
        Block unifiedPredicateToApply = conjunction(
                ImmutableList.of(
                        disjunction(dynamicPredicatesToApply, nameAllocator),
                        hoistCommonConjuncts(disjunction(staticPredicatesToApply, nameAllocator), nameAllocator)),
                nameAllocator);
        unifiedPredicateToApply = optimizeLogicalOperations(unifiedPredicateToApply);
        // remove the conjuncts that are already enforced
        unifiedPredicateToApply = removeConjuncts(unifiedPredicateToApply, enforcedPredicate, nameAllocator);

        // derive compensation predicates without dynamic filters
        ImmutableList.Builder<Block> residualPredicatesBuilder = ImmutableList.builder();
        for (Block staticPredicateToApply : staticPredicatesToApply) {
            staticPredicateToApply = removeConjuncts(staticPredicateToApply, enforcedPredicate, nameAllocator);
            staticPredicateToApply = removeConjuncts(staticPredicateToApply, unifiedPredicateToApply, nameAllocator);
            residualPredicatesBuilder.add(staticPredicateToApply);
        }
        List<Block> residualPredicates = residualPredicatesBuilder.build();

        Set<Integer> unifiedFieldsToPrune = contexts.stream()
                .map(TraversalContext::fieldsToPrune)
                .reduce((first, second) -> ImmutableSet.copyOf(Sets.intersection(first, second)))
                .orElseThrow();
        // do not prune fields if they are used by residual predicates
        // field indexes from different residual predicates are compatible because all predicates are based on baseUnifiedOperation
        Set<Integer> residualPredicateFields = residualPredicates.stream()
                .map(predicate -> extractReferencedFields(predicate, getOnlyElement(predicate.parameters())))
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        unifiedFieldsToPrune = Sets.difference(unifiedFieldsToPrune, residualPredicateFields);

        // build unified operations
        Operation unifiedOperation = baseUnifiedOperation;
        FieldMapping unifiedMapping = FieldMapping.identity(relationRowType(trinoType(unifiedOperation.result().type())));

        // add unified Filter operation
        boolean checkpointRequirement = false;
        if (!isTrue(unifiedPredicateToApply)) {
            unifiedOperation = new Filter(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    unifiedPredicateToApply.withLabel("^predicate"),
                    unifiedOperation.attributes());
            newOperations.put(unifiedOperation.result(), unifiedOperation);
            // set checkpoint after Filter only if there are static predicates
            DynamicFilterExtractionResult extractionResult = extractDynamicConjunct(unifiedPredicateToApply, nameAllocator);
            if (!isTrue(extractionResult.staticPredicate())) {
                checkpointRequirement = true;
            }
            // don't bother about mapping, filter is passthrough
        }

        // add unified Project operation
        if (!unifiedFieldsToPrune.isEmpty()) {
            Block pruningAssignments = getPruningAssignments("^assignments", relationRowType(trinoType(unifiedOperation.result().type())), unifiedFieldsToPrune, nameAllocator);
            unifiedOperation = new Project(
                    nameAllocator.newName(),
                    unifiedOperation.result(),
                    pruningAssignments,
                    unifiedOperation.attributes());
            unifiedMapping = unifiedMapping.composeWith(getPassthroughMapping(pruningAssignments));
            newOperations.put(unifiedOperation.result(), unifiedOperation);
        }

        // compute the new enforced predicate and rebase it onto the last unified operation.
        // Note: some conjuncts might no longer be supported after pruning and must be removed.
        Block newEnforcedPredicate = conjunction(ImmutableList.of(enforcedPredicate, unifiedPredicateToApply), nameAllocator);
        newEnforcedPredicate = rebasePredicateAndPruneUnsupportedConjuncts(newEnforcedPredicate, unifiedOperation, unifiedMapping, nameAllocator);

        // compute residual contexts and rebase them on the unified operation.
        ImmutableList.Builder<TraversalState> newStates = ImmutableList.builder();
        for (int i = 0; i < states.size(); i++) {
            TraversalState oldState = states.get(i);
            TraversalContext oldContext = oldState.traversalContext();

            // rebase the old mapping onto the last unified operation
            FieldMapping newFieldMapping = oldContext.fieldMapping().composeWith(unifiedMapping);

            // find the remaining fields to prune and rebase them onto the last unified operation
            Set<Integer> newFieldsToPrune = Sets.difference(oldContext.fieldsToPrune(), unifiedFieldsToPrune);
            newFieldsToPrune = remapIndexes(newFieldsToPrune, unifiedMapping);

            // rebase the residual predicate onto the last unified operation. Note: residual predicates are fully supported: all fields used by them were retained.
            Block newPredicateToApply = residualPredicates.get(i);
            newPredicateToApply = rebaseBlock(newPredicateToApply, relationRowType(trinoType(unifiedOperation.result().type())), unifiedMapping, nameAllocator).orElseThrow();

            TraversalContext newContext = new TraversalContext(newFieldMapping, newFieldsToPrune, newPredicateToApply, newEnforcedPredicate, oldContext.enforcedLimit());
            newStates.add(new TraversalState(newContext, oldState.nextOperation()));
        }

        return new UnifiedStatesAndCheckpointRequirement(new UnifiedStates(unifiedOperation, newStates.build()), checkpointRequirement);
    }

    public static Block rebasePredicateAndPruneUnsupportedConjuncts(Block block, Operation baseOperation, FieldMapping fieldMapping, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block optimizedPredicate = optimizeLogicalOperations(block);
        List<Block> conjuncts = extractConjuncts(optimizedPredicate, nameAllocator);

        Type baseRowType = relationRowType(trinoType(baseOperation.result().type()));
        List<Block> rebasedSupportedConjuncts = conjuncts.stream()
                .map(conjunct -> rebaseBlock(conjunct, baseRowType, fieldMapping, nameAllocator))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (rebasedSupportedConjuncts.isEmpty()) {
            return truePredicate(block.name(), ImmutableList.of(new Block.Parameter(nameAllocator.newName(), irType(baseRowType))), nameAllocator);
        }

        return conjunction(rebasedSupportedConjuncts, nameAllocator);
    }

    private static Set<Integer> remapIndexes(Set<Integer> indexes, FieldMapping fieldMapping)
    {
        return indexes.stream()
                .map(fieldMapping::get)
                .collect(toImmutableSet());
    }

    /**
     * Analyze the next operations in the branches and identify subgroups of branches that can be merged further.
     * <p>
     * The next operations fall into two categories:
     * 1. the operations whose all sources are in the group
     * Example: TopN (it has one source), self-Join (both sources are found in the group. Two branches point to the same Join operation as its left and right source)
     * 2. the operations that depend on operations from the group and on some other operations
     * Example: Join having only left source in the group
     * <p>
     * For the first category, we have all the necessary information to decide which operations can be merged.
     * For example, a TopN can be merged with any other identical TopN in the group.
     * A self-Join can be potentially transformed into Window, thus merging the two incoming branches.
     * When there are multiple identical self-Joins, they can be all merged together.
     * For the first category, {@link SingleGroupMerger} finds subgroups of branches to merge.
     * <p>
     * For the second category, we don't have the complete information about the operations: we miss the other sources.
     * For this category, {@link MultiGroupMerger} finds subgroups of branches to merge. It collects information
     * about branches which depend on multiple groups. When analyzing the new group, it can check for the
     * missing dependencies within the branches it has collected so far.
     * <p>
     * Notes on merging capabilities.
     * Whether the next operations are eligible for merging can be decided based on identity, but it can be more sophisticated.
     * For example, we can merge Aggregation operations having different aggregate functions.
     * Similarly, we can merge Project operations with different assignments.
     * Another example is a self-join, where two incoming branches can be merged only if we can transform the Join
     * into a single-source operation (Window).
     * Yet another example is self-Union or self-Exchange, where we must duplicate data from the merged branch.
     * <p>
     * Note that each branch has a TraversalContext representing the differences to be compensated.
     * When we merge the next operations, we must pull the TraversalContexts through the merged result,
     * which can block merging in some cases.
     */
    private static Subgroups identifySubgroupsToMerge(
            UnifiedStates unifiedStates,
            Map<Operation, Operation> operationToDownstream,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            MultiGroupMerger multiGroupMerger,
            PlannerContext plannerContext,
            Session session,
            Metadata metadata)
    {
        SingleGroupMerger.SingleGroupMergeDecomposition singleGroupSubgroups = SingleGroupMerger.identifySingleGroupSubgroupsToMerge(unifiedStates, nameAllocator);
        MultiGroupMerger.MultiGroupMergeDecomposition multiGroupSubgroups = multiGroupMerger.identifyMultiGroupSubgroupsToMerge(unifiedStates, operationToDownstream, nameAllocator, newOperations, plannerContext, session, metadata);

        Set<Integer> categorizedIndexes = Sets.union(singleGroupSubgroups.getIndexes(), multiGroupSubgroups.getIndexes());
        List<Integer> remainingIndexes = IntStream.range(0, unifiedStates.residualStates().size())
                .filter(index -> !categorizedIndexes.contains(index))
                .boxed()
                .collect(toImmutableList());

        return new Subgroups(singleGroupSubgroups.singleGroupMerges(), multiGroupSubgroups.multiGroupMerges(), multiGroupSubgroups.hangingBranches(), remainingIndexes);
    }

    private static void verifySubgroups(Subgroups subgroups, int branchesCount)
    {
        ImmutableMultiset.Builder<Integer> referencedStates = ImmutableMultiset.builder();
        subgroups.singleGroupMerges().stream()
                .forEach(referencedStates::addAll);
        subgroups.multiGroupMerges().stream()
                .map(MultiGroupMerger.MultiGroupMerge::newGroupBranches)
                .forEach(referencedStates::addAll);
        referencedStates.addAll(subgroups.hangingBranches());
        referencedStates.addAll(subgroups.remainingSingleGroupBranches());

        checkArgument(
                referencedStates.build().equals(ImmutableMultiset.copyOf(IntStream.range(0, branchesCount).boxed().collect(toImmutableSet()))),
                "each branch must be referenced exactly once");
    }

    /**
     * Restore the original semantics of the branch on top of the unifiedOperation:
     * - apply remaining predicate
     * - prune additional fields
     * - reorder fields to match the input type of the next downstream operation
     * Wire the new part of the plan to the old plan by replacing the argument of the next downstream operation
     * with the result of the last new operation.
     */
    public static void compensateAndWire(TraversalState branch, Operation unifiedOperation, ProgramBuilder.ValueNameAllocator nameAllocator, Map<Value, Operation> newOperations)
    {
        Operation recentOperation = unifiedOperation;
        FieldMapping mapping = branch.traversalContext().fieldMapping();

        // apply filter
        if (!isTrue(branch.traversalContext().predicateToApply())) {
            recentOperation = new Filter(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    branch.traversalContext().predicateToApply().withLabel("^predicate"),
                    recentOperation.attributes());
            newOperations.put(recentOperation.result(), recentOperation);
        }

        // apply pruning
        if (!branch.traversalContext().fieldsToPrune().isEmpty()) {
            Block pruningAssignments = getPruningAssignments("^assignments", relationRowType(trinoType(recentOperation.result().type())), branch.traversalContext().fieldsToPrune(), nameAllocator);
            recentOperation = new Project(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    pruningAssignments,
                    recentOperation.attributes());
            newOperations.put(recentOperation.result(), recentOperation);
            mapping = mapping.composeWith(getPassthroughMapping(pruningAssignments));
        }

        // reorder fields to match the next operation's input
        if (!mapping.isIdentity(relationRowType(trinoType(recentOperation.result().type())))) {
            recentOperation = new Project(
                    nameAllocator.newName(),
                    recentOperation.result(),
                    getReorderingAssignments(relationRowType(trinoType(recentOperation.result().type())), mapping, nameAllocator),
                    recentOperation.attributes());
            newOperations.put(recentOperation.result(), recentOperation);
        }

        // compensations are fully applied. Wire recentOperation to the nextOperation
        // the operation created by the withArgument() method has the same result as the original operation,
        // therefore it can be used by the downstream plan without further adjustments
        // Note: It is possible that multiple branches are wired to the same next operation (for example, if they are sources of the same union)
        Value nextOperationResult = branch.nextOperation().operation().result();
        Operation nextOperation = newOperations.containsKey(nextOperationResult) ? newOperations.get(nextOperationResult) : branch.nextOperation().operation();
        Operation nextOperationWired = ((TrinoOperation) nextOperation).withArgument(recentOperation.result(), branch.nextOperation().sourceIndex());
        newOperations.put(nextOperationWired.result(), nextOperationWired);
    }

    /**
     * Decide whether we should set a checkpoint at the next remote exchange after given operation.
     * <p>
     * The parts of the plan up to the checkpoint will be merged in the resulting plan, and if further merging
     * is not possible, the merged result will be spooled at the checkpoint. It is important to avoid setting checkpoints
     * where the cost of spooling is big compared to the gain of merging plans. For example, it makes no sense
     * to set a checkpoint after merging just table scans.
     * <p>
     * For now, we assume simple heuristics: we set a checkpoint after merging all kinds of operations except for
     * table scan and pruning projection.
     */
    private static boolean setCheckpointAfter(Operation operation)
    {
        return !(operation instanceof Project project && project.isPruning()) &&
                !(operation instanceof DynamicFilterSource);
    }

    /**
     * Build an updated block consisting of old and new operations.
     */
    private static Block layoutOperations(Block oldMainBlock, Map<Value, Operation> newOperations, boolean allowUnresolvedArguments)
    {
        // find the terminal operation of the resulting block
        Operation rootOperation = newOperations.values().stream()
                .filter(operation -> Objects.equals(operation.attributes().get(new AttributeKey(IR, TERMINAL)), true))
                .findFirst()
                .orElse(oldMainBlock.getTerminalOperation());

        Block.Builder newMainBlock = new Block.Builder(oldMainBlock.name(), oldMainBlock.parameters());

        layoutOperations(
                rootOperation,
                newOperations,
                oldMainBlock.operations().stream()
                        .collect(toImmutableMap(Operation::result, identity())),
                newMainBlock,
                new HashSet<>(),
                allowUnresolvedArguments);

        return newMainBlock.build();
    }

    /**
     * Build an updated block consisting of old and new operations.
     * <p>
     * Use case 1: Build a block representing the updated query with diamond shape.
     * <p>
     * The root operation is the Output operation, being the root of the query plan. Starting from this operation, we recursively output
     * the operation's sources, and then the operation itself. This way we assure the correct layout of the program where each value
     * is declared before it is used.
     * <p>
     * When searching for the source operations, we first check in the newOperations, and then in the oldOperations.
     * We find the operation by its result name. Generally, the oldOperations and the newOperations use different result names.
     * There is one exception to this rule: the operations on the border between the old parts of the program and the rewritten parts of the program
     * are new operations that reuse the old operation's result so that they fit in to the old program.
     * <p>
     * Because there is diamond shape, the common subqueries will be visited multiple times in this method.
     * We layout them once, on the first visit.
     * <p>
     * Use case 2: Build a block representing an updated filter predicate or an updated field selector after cleaning up dynamic filters.
     * For this case, we added the allowUnresolvedArguments option so that the operations in the block can refer to block parameters.
     *
     * @param operation -- the Output operation, being the root of the query plan
     * @param newOperations -- the relational operations created by the CTE reuse algorithm
     * @param oldOperations -- all the top-level relational operations from the original program
     * @param block -- a builder of the new top-level block
     * @param alreadyOutputOperations -- results of the operations that are already in the block
     * @param allowUnresolvedArguments -- informs whether all arguments of operations must refer to other operations (from newOperations or oldOperations). If false, other arguments are allowed, for example block parameters or correlated values.
     */
    private static void layoutOperations(Operation operation, Map<Value, Operation> newOperations, Map<Value, Operation> oldOperations, Block.Builder block, Set<Value> alreadyOutputOperations, boolean allowUnresolvedArguments)
    {
        if (!alreadyOutputOperations.contains(operation.result())) {
            for (Value value : operation.arguments()) {
                Operation source = newOperations.get(value);
                if (source == null) {
                    source = oldOperations.get(value);
                }
                if (source != null) {
                    layoutOperations(source, newOperations, oldOperations, block, alreadyOutputOperations, allowUnresolvedArguments);
                }
                else {
                    checkArgument(allowUnresolvedArguments, "source operation not found");
                }
            }
            block.addOperation(operation);
            alreadyOutputOperations.add(operation.result());
        }
    }

    /**
     * Unify dynamic filters whenever possible and remove the unsupported dynamic filters.
     * <p>
     * When subplans containing dynamic filters are merged, a disjunction of dynamic predicates is created.
     * Example:
     * one branch of the plan has predicate df1 AND df2 AND static_predicate_1,
     * another branch of the plan has predicate df3 AND df4 AND static_predicate_2.
     * After these branches are merged, the resulting subplan has filter:
     * ((df1 AND df2) OR (df3 AND df4)) AND (static_predicate_1 OR static_predicate_2).
     * The dynamic filters ((df1 AND df2) OR (df3 AND df4)) cannot be executed in this form.
     * Dynamic filters can only be executed if each dynamic filter forms a separate conjunct.
     * This transformation aims to extract dynamic filters as separate conjuncts so that they can be executed.
     * The dynamic filters that cannot be transformed this way, are removed both from Filter predicates,
     * and from assignments in Joins and DynamicFilterSources.
     * This transformation uses dynamic filter equivalence. Different Join operations and DynamicFilterSource operations can define sets of
     * globally unique dynamic filter assignments. When Join or DynamicFilterSource operations are merged, the resulting operation inherits
     * all dynamic filter assignments from the component operations. If two or more assignments refer to the same build side field,
     * such dynamic filters can be considered equivalent.
     * <p>
     * Transformation steps
     * 1. Identify equivalent dynamic filter ids. See {@link #getEquivalentDynamicFilters(Join)}, {@link #getEquivalentDynamicFilters(DynamicFilterSource)}.
     * For example, let's assume that {df1, df3} are identified as equivalent, because they are assigned in the same Join,
     * and refer to the same build side field.
     * 2. Rewrite dynamic filter references so that all equivalent ids are replaced with the same representative.
     * The example predicate is rewritten to:
     * ((df1 AND df2) OR (df1 AND df4)) AND (static_predicate_1 OR static_predicate_2)
     * 3. Hoist common dynamic conjuncts. See {@link PredicateUtils#hoistCommonConjuncts(Block, ProgramBuilder.ValueNameAllocator)}.
     * The example predicate is rewritten to:
     * df1 AND (df2 OR df4) AND (static_predicate_1 OR static_predicate_2)
     * 4. Remove the unsupported dynamic conjunct (where OR remains).
     * The example predicate is rewritten to:
     * df1 AND (static_predicate_1 OR static_predicate_2)
     * 5. Remove assignments for dynamic filters that are not used anymore from Join and DynamicFilterSource operations.
     * In the example, we should remove assignments for df2, df3, and df4.
     * <p>
     * Note: in this transformation, we only visit top-level Join, DynamicFilterSource and Filter operations. We don't support correlated queries.
     * <p>
     * Note: with merging DynamicFilterSource operations, it might happen that we unify the dynamic filter assignments on the build side,
     * but not unify the probe side of the join. In such a case this rewrite will result in a single (unified) dynamic filter being referenced
     * multiple times by the non-unified probe side operations. Multiple references to a dynamic filter are correct, and they occur without CTE reuse,
     * for example when a dynamic filter is pushed into all branches of a union.
     */
    private static Block cleanUpDynamicFilters(Block mainBlock, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        // initialize a collection of newly created operations
        Map<Value, Operation> newOperations = new HashMap<>();

        // collect dynamic filter ids from all Join and DynamicFilterSource operations, and group them by equivalence.
        List<EquivalentDynamicFilters> equivalenceGroups = mainBlock.operations().stream()
                .map(operation -> {
                    if (operation instanceof Join join) {
                        return getEquivalentDynamicFilters(join);
                    }
                    if (operation instanceof DynamicFilterSource dynamicFilterSource) {
                        return getEquivalentDynamicFilters(dynamicFilterSource);
                    }
                    return ImmutableList.<EquivalentDynamicFilters>of();
                })
                .flatMap(List::stream)
                .collect(toImmutableList());

        // process dynamic predicates in filters. Collect the ids of all retained dynamic filters
        ImmutableSet.Builder<String> retainedDynamicFilterIdsBuilder = ImmutableSet.builder();
        for (Operation operation : mainBlock.operations()) {
            if (operation instanceof Filter filter) {
                DynamicFilterExtractionResult extractionResult = extractDynamicConjunct(filter.predicate(), nameAllocator);
                Block dynamicPredicate = extractionResult.dynamicPredicate();
                if (isTrue(dynamicPredicate)) {
                    continue;
                }
                // rewrite dynamic filters based on equivalence
                Block rewrittenDynamicPredicate = rewriteDynamicFilterIds(dynamicPredicate, equivalenceGroups);
                // hoist common dynamic conjuncts
                Block hoistedDynamicPredicate = hoistCommonConjuncts(rewrittenDynamicPredicate, nameAllocator);
                // keep only those dynamic filters which form separate conjuncts
                List<Block> dynamicConjuncts = extractConjuncts(hoistedDynamicPredicate, nameAllocator).stream()
                        .filter(DynamicFilterUtils::isDynamicFilter)
                        .collect(toImmutableList());
                dynamicConjuncts.stream()
                        .map(DynamicFilterUtils::getDynamicFilterId)
                        .forEach(retainedDynamicFilterIdsBuilder::add);
                // combine dynamic conjuncts with the static part of the predicate
                Block newPredicate = optimizeLogicalOperations(conjunction(
                        ImmutableList.<Block>builder()
                                .addAll(dynamicConjuncts)
                                .add(extractionResult.staticPredicate())
                                .build(),
                        nameAllocator));
                Filter newFilter = new Filter(
                        filter.result().name(),
                        filter.argument(),
                        newPredicate,
                        ImmutableMap.of());
                newOperations.put(newFilter.result(), newFilter);
            }
        }
        Set<String> retainedDynamicFilterIds = retainedDynamicFilterIdsBuilder.build();

        // remove all unused dynamic filters from Join and DynamicFilterSource operations
        for (Operation operation : mainBlock.operations()) {
            if (operation instanceof Join join) {
                Join newJoin = removeDynamicFilterAssignments(join, retainedDynamicFilterIds, nameAllocator);
                newOperations.put(newJoin.result(), newJoin);
            }
            if (operation instanceof DynamicFilterSource dynamicFilterSource) {
                DynamicFilterSource newDynamicFilterSource = removeDynamicFilterAssignments(dynamicFilterSource, retainedDynamicFilterIds, nameAllocator);
                newOperations.put(newDynamicFilterSource.result(), newDynamicFilterSource);
            }
        }

        // build the new query plan with the modified Filter and Join operations
        Block prunedDynamicFiltersMainBlock = layoutOperations(mainBlock, newOperations, false);

        // remove DynamicFilterSource operations where all dynamic filter assignments have been pruned.
        // Replace the DynamicFilterSource operations with their source.
        Map<Value, Value> replacements = new HashMap<>();
        Set<Value> availableValues = new HashSet<>();
        Block.Builder newMainBlockBuilder = new Block.Builder(mainBlock.name(), mainBlock.parameters());
        for (Operation operation : prunedDynamicFiltersMainBlock.operations()) {
            if (operation instanceof DynamicFilterSource dynamicFilterSource && isEmptyFieldSelector(dynamicFilterSource.dynamicFilterTargetSelector())) {
                // do not output this operation, it should be replaced by its argument
                replacements.put(dynamicFilterSource.result(), dynamicFilterSource.argument());
            }
            else {
                // replace arguments and output the operation
                for (int i = 0; i < operation.arguments().size(); i++) {
                    Value argument = operation.arguments().get(i);
                    if (replacements.containsKey(argument)) {
                        Value newArgument = replacements.get(argument);
                        checkArgument(availableValues.contains(newArgument), "argument not available");
                        operation = ((TrinoOperation) operation).withArgument(newArgument, i);
                    }
                }
                newMainBlockBuilder.addOperation(operation);
                availableValues.add(operation.result());
            }
        }

        return newMainBlockBuilder.build();
    }

    /**
     * Group dynamic filters assigned in given Join by equivalence.
     * Two dynamic filter ids are equivalent if they are assigned in the same Join, and refer to the same build side field.
     * Such situation is possible when the Join is a result of merging multiple Join operations.
     * When merging Join operations, their dynamic filter assignments are concatenated.
     */
    private static List<EquivalentDynamicFilters> getEquivalentDynamicFilters(Join join)
    {
        return getEquivalentDynamicFilters(JoinOperationMetadata.DYNAMIC_FILTER_IDS.getAttribute(join.attributes()), join.dynamicFilterTargetSelector());
    }

    /**
     * Group dynamic filters assigned in given DynamicFilterSource by equivalence.
     * Two dynamic filter ids are equivalent if they are assigned in the same DynamicFilterSource, and refer to the same input field.
     * Such situation is possible when the DynamicFilterSource is a result of merging multiple DynamicFilterSource operations.
     * When merging DynamicFilterSource operations, their dynamic filter assignments are concatenated.
     */
    private static List<EquivalentDynamicFilters> getEquivalentDynamicFilters(DynamicFilterSource dynamicFilterSource)
    {
        return getEquivalentDynamicFilters(DynamicFilterSourceOperationMetadata.DYNAMIC_FILTER_IDS.getAttribute(dynamicFilterSource.attributes()), dynamicFilterSource.dynamicFilterTargetSelector());
    }

    private static List<EquivalentDynamicFilters> getEquivalentDynamicFilters(List<String> dynamicFilterIds, Block dynamicFilterTargetSelector)
    {
        List<Integer> targetFields = getSelectedFields(dynamicFilterTargetSelector);

        ImmutableListMultimap.Builder<Integer, String> idsForField = ImmutableListMultimap.builder();
        for (int i = 0; i < targetFields.size(); i++) {
            idsForField.put(targetFields.get(i), dynamicFilterIds.get(i));
        }

        return idsForField.build().asMap().values().stream()
                .map(ids -> new EquivalentDynamicFilters(
                        ImmutableSet.copyOf(ids),
                        ids.stream().findFirst().orElseThrow()))
                .collect(toImmutableList());
    }

    /**
     * Rewrite dynamic filters based on equivalence. Replace each dynamic filter id with a representative of its equivalence group.
     * Initially, the predicate should be of the following form: (df1 AND df2) OR (df3 AND df4 AND df5) OR ..., where each disjunct comes from one merged branch.
     * For equivalence groups: {df1, df4}, {df2, df5}, the rewritten predicate will be: (df1 AND df2) OR (df3 AND df1 AND df2) OR ...
     * <p>
     * Note: this method only visits the top-level operations in the predicate block. We don't expect dynamic filters on nested level in the predicate.
     */
    private static Block rewriteDynamicFilterIds(Block predicate, List<EquivalentDynamicFilters> equivalenceGroups)
    {
        Map<Value, Operation> newOperations = new HashMap<>();

        Map<Value, Operation> operations = predicate.operations().stream()
                .collect(toImmutableMap(Operation::result, identity()));

        predicate.operations().stream()
                .forEach(operation -> {
                    if (isDynamicFilterFunction(operation)) {
                        // get the dynamic filter id
                        Value idArgument = operation.arguments().get(2);
                        Operation idOperation = operations.get(idArgument);
                        checkArgument(idOperation instanceof Constant, "expected dynamic filter id to be constant");
                        ConstantValue idAttribute = CONSTANT_VALUE.getAttribute(idOperation.attributes());
                        checkArgument(idAttribute.getType().equals(VARCHAR), "expected dynamic filter id to be of varchar type");
                        String id = ((Slice) idAttribute.getValue()).toStringUtf8();
                        // find the equivalence group of the id, and get the group representative
                        String representative = equivalenceGroups.stream()
                                .filter(group -> group.ids().contains(id))
                                .map(EquivalentDynamicFilters::representative)
                                .findFirst()
                                .orElseThrow();
                        // replace the id with the group representative
                        if (!representative.equals(id)) {
                            Operation newIdOperation = new Constant(idOperation.result().name(), VARCHAR, Slices.utf8Slice(representative));
                            newOperations.put(newIdOperation.result(), newIdOperation);
                        }
                    }
                });

        return layoutOperations(predicate, newOperations, true);
    }

    private static Join removeDynamicFilterAssignments(Join join, Set<String> retainedDynamicFilterIds, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<String> dynamicFilterIds = JoinOperationMetadata.DYNAMIC_FILTER_IDS.getAttribute(join.attributes());
        Block dynamicFilterTargetSelector = join.dynamicFilterTargetSelector();

        if (isEmptyFieldSelector(dynamicFilterTargetSelector)) {
            return join;
        }

        DynamicFilterAssignments result = removeDynamicFilterAssignments(dynamicFilterIds, dynamicFilterTargetSelector, retainedDynamicFilterIds, nameAllocator);

        return new Join(
                join.result().name(),
                join.arguments().get(0),
                join.arguments().get(1),
                join.leftCriteriaSelector(),
                join.rightCriteriaSelector(),
                join.filter(),
                join.leftOutputSelector(),
                join.rightOutputSelector(),
                result.dynamicFilterTargetSelector(),
                JOIN_TYPE.getAttribute(join.attributes()),
                MAY_SKIP_OUTPUT_DUPLICATES.getAttribute(join.attributes()),
                Optional.ofNullable(DISTRIBUTION_TYPE.getAttribute(join.attributes())),
                Optional.ofNullable(SPILLABLE.getAttribute(join.attributes())),
                result.dynamicFilterIds(),
                Optional.ofNullable(STATISTICS_AND_COST_SUMMARY.getAttribute(join.attributes())),
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    private static DynamicFilterSource removeDynamicFilterAssignments(DynamicFilterSource dynamicFilterSource, Set<String> retainedDynamicFilterIds, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        List<String> dynamicFilterIds = DynamicFilterSourceOperationMetadata.DYNAMIC_FILTER_IDS.getAttribute(dynamicFilterSource.attributes());
        Block dynamicFilterTargetSelector = dynamicFilterSource.dynamicFilterTargetSelector();

        if (isEmptyFieldSelector(dynamicFilterTargetSelector)) {
            return dynamicFilterSource;
        }

        DynamicFilterAssignments result = removeDynamicFilterAssignments(dynamicFilterIds, dynamicFilterTargetSelector, retainedDynamicFilterIds, nameAllocator);

        return new DynamicFilterSource(
                dynamicFilterSource.result().name(),
                getOnlyElement(dynamicFilterSource.arguments()),
                result.dynamicFilterTargetSelector(),
                result.dynamicFilterIds(),
                ImmutableMap.of());
    }

    private static DynamicFilterAssignments removeDynamicFilterAssignments(List<String> dynamicFilterIds, Block dynamicFilterTargetSelector, Set<String> retainedDynamicFilterIds, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Row row = (Row) dynamicFilterTargetSelector.operations().get(dynamicFilterTargetSelector.operations().size() - 2);
        ImmutableList.Builder<String> newDynamicFilterIdsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Value> newSelectedFieldsBuilder = ImmutableList.builder();
        for (int i = 0; i < dynamicFilterIds.size(); i++) {
            if (retainedDynamicFilterIds.contains(dynamicFilterIds.get(i))) {
                newDynamicFilterIdsBuilder.add(dynamicFilterIds.get(i));
                newSelectedFieldsBuilder.add(row.arguments().get(i));
            }
        }
        List<String> newDynamicFilterIds = newDynamicFilterIdsBuilder.build();
        List<Value> newSelectedFields = newSelectedFieldsBuilder.build();
        Block newDynamicFilterTargetSelector;
        if (newSelectedFields.isEmpty()) {
            newDynamicFilterTargetSelector = getEmptyFieldSelector(
                    dynamicFilterTargetSelector.name().orElseThrow(),
                    trinoType(getOnlyElement(dynamicFilterTargetSelector.parameters()).type()),
                    nameAllocator);
        }
        else {
            Row newRow = new Row(nameAllocator.newName(), newSelectedFields, emptySourceAttributes(newSelectedFields.size()));
            Return newReturn = new Return(nameAllocator.newName(), newRow.result(), newRow.attributes());
            newDynamicFilterTargetSelector = layoutOperations(
                    dynamicFilterTargetSelector,
                    ImmutableMap.of(newRow.result(), newRow, newReturn.result(), newReturn),
                    true);
        }

        return new DynamicFilterAssignments(newDynamicFilterIds, newDynamicFilterTargetSelector);
    }

    private record DynamicFilterAssignments(List<String> dynamicFilterIds, Block dynamicFilterTargetSelector)
    {
        private DynamicFilterAssignments
        {
            dynamicFilterIds = ImmutableList.copyOf(dynamicFilterIds);
            requireNonNull(dynamicFilterTargetSelector, "dynamicFilterTargetSelector is null");
        }
    }

    private record EquivalentDynamicFilters(Set<String> ids, String representative)
    {
        private EquivalentDynamicFilters
        {
            requireNonNull(ids, "ids is null");
            requireNonNull(representative, "representative is null");
            checkArgument(ids.contains(representative), "representative not in set");
            ids = ImmutableSet.copyOf(ids);
        }
    }

    /**
     * A group of compatible tables with unification results.
     *
     * @param tableScans -- a list of unified TableScans in the order of unification
     * @param unificationResults -- a list of partial and final unification results in the order of unification
     */
    public record UnifiedGroup(List<TableScan> tableScans, List<UnificationResult<TableHandle>> unificationResults)
    {
        public UnifiedGroup
        {
            requireNonNull(tableScans, "tableScans is null");
            requireNonNull(unificationResults, "unificationResults is null");
            checkArgument(tableScans.size() == unificationResults.size() + 1, "the number of unified tables does not match the number of unification results");
        }
    }

    /**
     * The branch-specific context for the CTE reuse traversal. Logically, it is applicable on top of the recently processed operation in this branch.
     *
     * @param fieldMapping -- the output type of the recent operation might have changed. The mapping serves to update the next operation accordingly
     * @param fieldsToPrune -- additional fields output by the recent operation as the result of unifying with other branches
     * @param predicateToApply -- predicate extracted from this branch as the result of unifying with other branches. Note: it might use fields marked as fieldsToPrune.
     * The predicate is assumed to be optimized. It is based on the recent unified operation type.
     * @param enforcedPredicate -- predicate guaranteed for the unified plan. Note: it might not be the full guaranteed predicate.
     * It only contains the conjuncts supported by the output fields of the recent operation. It is used when creating new predicates to avoid repetition.
     * The predicate is assumed to be optimized. It is based on the recent unified operation type.
     * @param enforcedLimit -- limit guaranteed for the unified plan
     */
    public record TraversalContext(FieldMapping fieldMapping, Set<Integer> fieldsToPrune, Block predicateToApply, Block enforcedPredicate, OptionalLong enforcedLimit)
    {
        public TraversalContext
        {
            requireNonNull(fieldMapping, "mapping is null");
            requireNonNull(fieldsToPrune, "fieldsToPrune is null");
            requireNonNull(predicateToApply, "predicateToApply is null");
            requireNonNull(enforcedPredicate, "enforcedPredicate is null");
            requireNonNull(enforcedLimit, "enforcedLimit is null");
            fieldsToPrune = ImmutableSet.copyOf(fieldsToPrune);
        }
    }

    /**
     * Traversal state for the branch. It consists of traversal context and the next operation in the branch.
     */
    public record TraversalState(TraversalContext traversalContext, OperationAndIndex nextOperation)
    {
        public TraversalState
        {
            requireNonNull(traversalContext, "traversalContext is null");
            requireNonNull(nextOperation, "nextOperation is null");
        }
    }

    /**
     * Traversal state for the merged group. It consists of the common unified operation and traversal states for all branches in the group.
     */
    public record UnifiedStates(Operation unifiedOperation, List<TraversalState> residualStates)
    {
        public UnifiedStates
        {
            requireNonNull(unifiedOperation, "unifiedOperation is null");
            requireNonNull(residualStates, "residualStates is null");
            checkArgument(!residualStates.isEmpty(), "residualStates is empty");
            residualStates = ImmutableList.copyOf(residualStates);
        }
    }

    private record UnifiedStatesAndCheckpointRequirement(UnifiedStates unifiedStates, boolean requireCheckpoint)
    {
        private UnifiedStatesAndCheckpointRequirement
        {
            requireNonNull(unifiedStates, "unifiedStates is null");
        }
    }

    public record UnifiedStatesAndCheckpointMapping(UnifiedStates unifiedStates, List<Checkpoint> checkpoints, BranchesToCheckpointsMapping branchToCheckpoint)
    {
        public UnifiedStatesAndCheckpointMapping
        {
            requireNonNull(unifiedStates, "unifiedStates is null");
            requireNonNull(checkpoints, "checkpoints is null");
            requireNonNull(branchToCheckpoint, "branchToCheckpoint is null");
            checkpoints = ImmutableList.copyOf(checkpoints);
        }
    }

    /**
     * Indexes of branches categorized for merging.
     *
     * @param singleGroupMerges -- groups of branches to be merged. These branches have no dependencies outside the group
     * @param multiGroupMerges -- groups of branches to be merged together with their dependencies outside the group
     * @param hangingBranches -- a group of branches with yet unknown merging potential. These branches have dependencies outside the group
     * @param remainingSingleGroupBranches -- branches that cannot be merged. These branches have no dependencies outside the group
     */
    private record Subgroups(
            List<List<Integer>> singleGroupMerges,
            List<MultiGroupMerger.MultiGroupMerge> multiGroupMerges,
            List<Integer> hangingBranches,
            List<Integer> remainingSingleGroupBranches)
    {
        private Subgroups
        {
            requireNonNull(singleGroupMerges, "singleGroupMerges is null");
            requireNonNull(multiGroupMerges, "multiGroupMerges is null");
            requireNonNull(hangingBranches, "hangingBranches is null");
            requireNonNull(remainingSingleGroupBranches, "remainingSingleGroupBranches is null");
            checkArgument(
                    singleGroupMerges.stream()
                            .allMatch(indexes -> indexes.size() > 1),
                    "each subgroup must have at least 2 branches");
            singleGroupMerges = singleGroupMerges.stream()
                    .map(ImmutableList::copyOf)
                    .collect(toImmutableList());
            multiGroupMerges = ImmutableList.copyOf(multiGroupMerges);
            hangingBranches = ImmutableList.copyOf(hangingBranches);
            remainingSingleGroupBranches = ImmutableList.copyOf(remainingSingleGroupBranches);
        }
    }

    /**
     * Simplified view of scope in the query plan.
     * It consists of operation results and block parameters which are visible and can be correctly used as arguments.
     *
     * @param operationResults - accessible operation results mapped to the operations that return them.
     * In Trino plan, we can only access the results of preceding operations in the same block.
     * Although certain operation results from outer blocks are visible, they cannot be referenced in arguments,
     * and are not present in this map.
     * @param blockParameters - accessible block parameters mapped to blocks that declare them.
     * It contains parameters of the current block and parameters of all outer blocks.
     */
    private record AccessibleValueMap(Map<Operation.Result, Operation> operationResults, Map<Block.Parameter, Block> blockParameters)
    {
        private AccessibleValueMap
        {
            requireNonNull(operationResults, "operationResults is null");
            requireNonNull(blockParameters, "blockParameters is null");
            operationResults = ImmutableMap.copyOf(operationResults);
            blockParameters = ImmutableMap.copyOf(blockParameters);
        }

        public static AccessibleValueMap initialize()
        {
            return new AccessibleValueMap(ImmutableMap.of(), ImmutableMap.of());
        }

        public AccessibleValueMap withOperationResult(Operation operation)
        {
            Map<Operation.Result, Operation> newOperationResults = new HashMap<>(operationResults);
            if (newOperationResults.put(operation.result(), operation) != null) {
                throw new TrinoException(IR_ERROR, format("Operation result %s already in scope", operation.result().name()));
            }
            return new AccessibleValueMap(newOperationResults, blockParameters);
        }

        public AccessibleValueMap forNestedBlock(Block block)
        {
            Map<Block.Parameter, Block> newBlockParameters = new HashMap<>(blockParameters);
            for (Block.Parameter parameter : block.parameters()) {
                if (newBlockParameters.put(parameter, block) != null) {
                    throw new TrinoException(IR_ERROR, format("Block parameter %s already in scope", parameter.name()));
                }
            }
            // clear outer operation results. They mustn't be referenced in nested blocks.
            return new AccessibleValueMap(ImmutableMap.of(), newBlockParameters);
        }

        public SourceNode getSource(Value value)
        {
            if (value instanceof Operation.Result result) {
                Operation operation = operationResults.get(result);
                if (operation != null) {
                    return operation;
                }
            }
            if (value instanceof Block.Parameter parameter) {
                Block block = blockParameters.get(parameter);
                if (block != null) {
                    return block;
                }
            }
            throw new TrinoException(IR_ERROR, format("Value %s not in scope", value.name()));
        }
    }
}
