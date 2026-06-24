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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.scheduler.PipelinedQueryScheduler.BucketToPartitionKey;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.LanguageFunctionProvider.LanguageFunctionData;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.operator.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.function.FunctionId;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.newir.Program;
import io.trino.sql.planner.AdaptivePlanner.ExchangeSourceId;
import io.trino.sql.planner.newirtoold.NewIrFragmenter;
import io.trino.sql.planner.newirtoold.NewIrFragmenter.NewIrPartitioningScheme;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.transaction.TransactionManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getQueryMaxStageCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isForceSingleNodeOutput;
import static io.trino.execution.scheduler.PipelinedQueryScheduler.getKeyForFragment;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static io.trino.spi.connector.StandardWarningCode.TOO_MANY_STAGES;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getEmptyFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonFragmentPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private static final String TOO_MANY_STAGES_MESSAGE = "" +
            "If the query contains multiple aggregates with DISTINCT over different columns, please set the 'distinct_aggregations_strategy' session property to 'single_step'. " +
            "If the query contains WITH clauses that are referenced more than once, please create temporary table(s) for the queries in those clauses.";
    private static final Logger log = Logger.get(PlanFragmenter.class);

    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TransactionManager transactionManager;
    private final CatalogManager catalogManager;
    private final LanguageFunctionManager languageFunctionManager;
    private final int stageCountWarningThreshold;

    @Inject
    public PlanFragmenter(
            Metadata metadata,
            FunctionManager functionManager,
            TransactionManager transactionManager,
            CatalogManager catalogManager,
            LanguageFunctionManager languageFunctionManager,
            QueryManagerConfig queryManagerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.stageCountWarningThreshold = requireNonNull(queryManagerConfig, "queryManagerConfig is null").getStageCountWarningThreshold();
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, WarningCollector warningCollector)
    {
        return createSubPlans(
                session,
                plan,
                forceSingleNode,
                warningCollector,
                new PlanFragmentIdAllocator(0),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()),
                ImmutableMap.of());
    }

    public SubPlan createSubPlans(
            Session session,
            Plan plan,
            boolean forceSingleNode,
            WarningCollector warningCollector,
            PlanFragmentIdAllocator idAllocator,
            PartitioningScheme outputPartitioningScheme,
            Map<ExchangeSourceId, SubPlan> unchangedSubPlans)
    {
        List<CatalogProperties> activeCatalogs = transactionManager.getActiveCatalogs(session.getTransactionId().orElseThrow()).stream()
                .map(CatalogInfo::catalogHandle)
                .flatMap(catalogHandle -> catalogManager.getCatalogProperties(catalogHandle).stream())
                .collect(toImmutableList());
        Fragmenter fragmenter = new Fragmenter(
                session,
                metadata,
                functionManager,
                plan.getStatsAndCosts(),
                activeCatalogs,
                languageFunctionManager.serializeFunctionsForWorkers(session),
                idAllocator,
                unchangedSubPlans);
        FragmentProperties properties = new FragmentProperties(outputPartitioningScheme);
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        subPlan = reassignPartitioningHandleIfNecessary(session, subPlan).orElseThrow(() -> new IllegalStateException("Failed to reassign partitioning handles for non-diamond plan"));

        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(subPlan, warningCollector, getQueryMaxStageCount(session), stageCountWarningThreshold);

        return subPlan;
    }

    /**
     * Fragment the plan which is represented in the new IR, and potentially involves diamond shape.
     * The returned representation of the fragmented plan uses the old IR.
     */
    public Optional<SubPlan> createSubPlans(Session session, Program program, boolean forceSingleNode, WarningCollector warningCollector)
    {
        List<CatalogProperties> activeCatalogs = transactionManager.getActiveCatalogs(session.getTransactionId().orElseThrow()).stream()
                .map(CatalogInfo::catalogHandle)
                .flatMap(catalogHandle -> catalogManager.getCatalogProperties(catalogHandle).stream())
                .collect(toImmutableList());

        // build output partitioning scheme using new ValueNameAllocator.
        // The created NewIrPartitioningScheme will not be used in the context of the existing program, so there cannot be value name clashes.
        // The NewIrPartitioningScheme will be translated to old IR before it is incorporated into the fragmented plan.
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Output outputOperation = (Output) ((Query) program.root()).query().getTerminalOperation();
        Type outputRowType = trinoType(outputOperation.outputFieldSelector().getReturnedType());
        NewIrPartitioningScheme outputPartitioningScheme = new NewIrPartitioningScheme(
                getFullPassthroughFieldSelector("^outputLayoutSelector", outputRowType, nameAllocator),
                SINGLE_DISTRIBUTION,
                getEmptyFieldSelector("^boundArguments", outputRowType, nameAllocator),
                false,
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty());

        FragmentProperties properties = new FragmentProperties(outputPartitioningScheme);
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }

        // The old IR fragmenter takes the unchangedSubPlans map. It is used for adaptive re-planning. The new IR fragmenter is only used for initial plan fragmenting, so it does not need this map.
        // The old IR fragmenter takes StatsAndCosts. We do not pass StatsAndCosts to the new IR fragmenter, and use 'empty' values instead.
        // TODO We should assign a PlanNodeId to each relational operation in the rewritten plan, calculate stats and cost for those operations, and make the StatsAndCosts map.
        NewIrFragmenter fragmenter = new NewIrFragmenter(session, metadata, functionManager, activeCatalogs, languageFunctionManager.serializeFunctionsForWorkers(session));

        SubPlan subPlan = fragmenter.fragmentProgram(program, properties);
        Optional<SubPlan> reassignedSubPlan = reassignPartitioningHandleIfNecessary(session, subPlan);
        if (reassignedSubPlan.isEmpty()) {
            return Optional.empty();
        }
        subPlan = reassignedSubPlan.get();
        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // If multiple downstream fragments read from the same upstream fragment, all the downstream fragments must have matching bucket-to-partition requirements.
        ImmutableMultimap.Builder<PlanFragmentId, BucketToPartitionKey> requirementsBuilder = ImmutableMultimap.builder();
        collectDownstreamRequirements(subPlan, requirementsBuilder, new HashSet<>(), session);
        for (Map.Entry<PlanFragmentId, Collection<BucketToPartitionKey>> entry : requirementsBuilder.build().asMap().entrySet()) {
            if (entry.getValue().stream().distinct().count() > 1) {
                log.info("Failed to reconcile downstream bucket-to-partitioning for fragment: %s, query: %s", entry.getKey(), session.getQueryId());
                return Optional.empty();
            }
        }

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(subPlan, warningCollector, getQueryMaxStageCount(session), stageCountWarningThreshold);

        return Optional.of(subPlan);
    }

    // collect all downstream bucket-to-partition requirements for each fragment in the plan
    private static void collectDownstreamRequirements(SubPlan root, ImmutableMultimap.Builder<PlanFragmentId, BucketToPartitionKey> builder, Set<PlanFragmentId> processedFragments, Session session)
    {
        PlanFragment fragment = root.getFragment();
        PlanFragmentId fragmentId = fragment.getId();

        if (processedFragments.contains(fragmentId)) {
            return;
        }

        // record this fragment's requirement for all upstream fragments
        BucketToPartitionKey bucketToPartitionKey = getKeyForFragment(fragment, session);
        fragment.getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .forEach(upstreamFragmentId -> builder.put(upstreamFragmentId, bucketToPartitionKey));

        // recurse into upstream fragments
        for (SubPlan upstreamSubPlan : root.getChildren()) {
            collectDownstreamRequirements(upstreamSubPlan, builder, processedFragments, session);
        }

        processedFragments.add(fragmentId);
    }

    private void sanityCheckFragmentedPlan(SubPlan subPlan, WarningCollector warningCollector, int maxStageCount, int stageCountSoftLimit)
    {
        subPlan.sanityCheck();
        int fragmentCount = subPlan.getAllFragments().size();
        if (fragmentCount > maxStageCount) {
            throw new TrinoException(QUERY_HAS_TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the allowed maximum (%s). %s",
                    fragmentCount,
                    maxStageCount,
                    TOO_MANY_STAGES_MESSAGE));
        }
        if (fragmentCount > stageCountSoftLimit) {
            warningCollector.add(new TrinoWarning(TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the soft limit (%s). %s",
                    fragmentCount,
                    stageCountSoftLimit,
                    TOO_MANY_STAGES_MESSAGE)));
        }
    }

    private Optional<SubPlan> reassignPartitioningHandleIfNecessary(Session session, SubPlan subPlan)
    {
        return reassignPartitioningHandleIfNecessaryHelper(session, subPlan, subPlan.getFragment().getPartitioning(), new HashMap<>());
    }

    private Optional<SubPlan> reassignPartitioningHandleIfNecessaryHelper(Session session, SubPlan subPlan, PartitioningHandle newOutputPartitioningHandle, Map<PlanFragmentId, ProcessedSubPlan> processedSubPlans)
    {
        PlanFragment fragment = subPlan.getFragment();

        // this fragment could have been already processed as the result of diamond shape
        ProcessedSubPlan processedSubPlan = processedSubPlans.get(fragment.getId());
        if (processedSubPlan != null) {
            // we're visiting the same fragment again. we can only proceed if the results of each visit are identical.
            // it happens when:
            // - the fragment's output partitioning handle cannot be effectively updated (because it is a system handle)
            // - the fragment's output partitioning handle was updated, and the new suggested partitioning handle is the same as the updated one
            if (processedSubPlan.updatedPartitioningHandle().isEmpty() ||
                    processedSubPlan.updatedPartitioningHandle().get().equals(newOutputPartitioningHandle)) {
                return Optional.of(processedSubPlan.result());
            }
            // bail out because of incompatible output partitioning requests
            log.info("Failed to reconcile output partitioning for fragment: %s, query: %s", fragment.getId(), session.getQueryId());
            return Optional.empty();
        }

        PlanNode newRoot = fragment.getRoot();
        // If the fragment's partitioning is SINGLE or COORDINATOR_ONLY, leave the sources as is (this is for single-node execution)
        if (!fragment.getPartitioning().isSingleNode()) {
            PartitioningHandleReassigner partitioningHandleReassigner = new PartitioningHandleReassigner(fragment.getPartitioning(), metadata, session);
            newRoot = SimplePlanRewriter.rewriteWith(partitioningHandleReassigner, newRoot);
        }
        PartitioningScheme outputPartitioningScheme = fragment.getOutputPartitioningScheme();
        Partitioning newOutputPartitioning = outputPartitioningScheme.getPartitioning();
        Optional<PartitioningHandle> updatedPartitioningHandle = Optional.empty();
        if (outputPartitioningScheme.getPartitioning().getHandle().getCatalogHandle().isPresent()) {
            // Do not replace the handle if the source's output handle is a system one, e.g. broadcast.
            newOutputPartitioning = newOutputPartitioning.withAlternativePartitioningHandle(newOutputPartitioningHandle);
            updatedPartitioningHandle = Optional.of(newOutputPartitioningHandle);
        }
        PlanFragment newFragment = new PlanFragment(
                fragment.getId(),
                newRoot,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionCount(),
                fragment.getPartitionedSources(),
                new PartitioningScheme(
                        newOutputPartitioning,
                        outputPartitioningScheme.getOutputLayout(),
                        outputPartitioningScheme.isReplicateNullsAndAny(),
                        outputPartitioningScheme.getBucketToPartition(),
                        outputPartitioningScheme.getBucketCount(),
                        outputPartitioningScheme.getPartitionCount()),
                OptionalInt.empty(),
                fragment.getStatsAndCosts(),
                fragment.getActiveCatalogs(),
                fragment.getLanguageFunctions(),
                fragment.getJsonRepresentation());

        ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            Optional<SubPlan> processedChild = reassignPartitioningHandleIfNecessaryHelper(session, child, fragment.getPartitioning(), processedSubPlans);
            if (processedChild.isPresent()) {
                childrenBuilder.add(processedChild.get());
            }
            else {
                return Optional.empty();
            }
        }
        SubPlan newSubPlan = new SubPlan(newFragment, childrenBuilder.build());
        processedSubPlans.put(fragment.getId(), new ProcessedSubPlan(newSubPlan, updatedPartitioningHandle));

        return Optional.of(newSubPlan);
    }

    private record ProcessedSubPlan(SubPlan result, Optional<PartitioningHandle> updatedPartitioningHandle)
    {
        private ProcessedSubPlan
        {
            requireNonNull(result, "result is null");
            requireNonNull(updatedPartitioningHandle, "updatedPartitioningHandle is null");
        }
    }

    public static boolean isWorkerCoordinatorBoundary(FragmentProperties fragmentProperties, List<FragmentProperties> childFragmentsProperties)
    {
        if (!fragmentProperties.getPartitioningHandle().isCoordinatorOnly()) {
            // receiver stage is not a coordinator stage
            return false;
        }
        if (childFragmentsProperties.stream().allMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly())) {
            // coordinator to coordinator exchange
            return false;
        }
        checkArgument(
                childFragmentsProperties.stream().noneMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly()),
                "Plans are not expected to have a mix of coordinator only fragments and distributed fragments as siblings");
        return true;
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private final Session session;
        private final Metadata metadata;
        private final FunctionManager functionManager;
        private final StatsAndCosts statsAndCosts;
        private final List<CatalogProperties> activeCatalogs;
        private final Map<FunctionId, LanguageFunctionData> languageFunctions;
        private final PlanFragmentIdAllocator idAllocator;
        private final Map<ExchangeSourceId, SubPlan> unchangedSubPlans;
        private final PlanFragmentId rootFragmentID;

        public Fragmenter(
                Session session,
                Metadata metadata,
                FunctionManager functionManager,
                StatsAndCosts statsAndCosts,
                List<CatalogProperties> activeCatalogs,
                Map<FunctionId, LanguageFunctionData> languageFunctions,
                PlanFragmentIdAllocator idAllocator,
                Map<ExchangeSourceId, SubPlan> unchangedSubPlans)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
            this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
            this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
            this.languageFunctions = ImmutableMap.copyOf(languageFunctions);
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.unchangedSubPlans = ImmutableMap.copyOf(requireNonNull(unchangedSubPlans, "unchangedSubPlans is null"));
            this.rootFragmentID = idAllocator.getNextId();
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, rootFragmentID);
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(root);

            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    dependencies,
                    properties.getPartitioningHandle(),
                    properties.getPartitionCount(),
                    schedulingOrder,
                    properties.getPartitioningScheme(),
                    OptionalInt.empty(),
                    statsAndCosts.getForSubplan(root),
                    activeCatalogs,
                    languageFunctions,
                    Optional.of(jsonFragmentPlan(root, metadata, functionManager, session)));

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.get().setSingleNodeDistribution();
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSimpleTableExecuteNode(SimpleTableExecuteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableDelete(TableDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableUpdate(TableUpdateNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, node.getTable())
                    .getTablePartitioning()
                    .filter(_ -> node.isUseConnectorNodePartitioning())
                    .map(TablePartitioning::partitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);

            context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitRefreshMaterializedView(RefreshMaterializedViewNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableExecute(TableExecuteNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getRowCount() != 0) {
                // A non-empty values node requires single distribution
                context.get().setSingleNodeDistribution();
            }
            else {
                // An empty values node is compatible with any distribution, so
                // do not overwrite a distribution if there is one already chosen,
                // and delay setting the distribution in case the fragment contains
                // another node with specific distribution requirements
                context.get().setContainsEmptyValues();
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFunction(TableFunctionNode node, RewriteContext<FragmentProperties> context)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getSource().isEmpty()) {
                // context is mutable. The leaf node should set the PartitioningHandle.
                context.get().addSourceDistribution(node.getId(), SOURCE_DISTRIBUTION, metadata, session);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitChooseAlternativeNode(ChooseAlternativeNode node, RewriteContext<FragmentProperties> context)
        {
            // partitioning shouldn't change when creating alternatives, therefore the original table's partitioning should fit all alternatives
            TableScanNode scan = node.getOriginalTableScan().tableScanNode();
            PartitioningHandle partitioning = metadata.getTableProperties(session, scan.getTable())
                    .getTablePartitioning()
                    .filter(_ -> scan.isUseConnectorNodePartitioning())
                    .map(TablePartitioning::partitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);

            // stop the process in order not to add the underlying TableScanNodes as well
            return node;
        }

        @Override
        public PlanNode visitAdaptivePlanNode(AdaptivePlanNode node, RewriteContext<FragmentProperties> context)
        {
            // This is needed to make the initial plan more concise by replacing the exchange nodes with
            // remote source nodes for stages that are not being changed by the adaptive planner in the
            // case of FTE. This is a cosmetic change and does not affect the execution of the plan. This is
            // useful for easier debugging and understanding of the plan.
            // Example:
            //   - Before:
            //     - AdaptivePlan
            //       - InitialPlan
            //         - SomeInitialPlanNode
            //           - Exchange
            //             - TableScan
            //       - CurrentPlan
            //         - NewPlanNode
            //           - RemoteSourceNode(1)
            //    - After:
            //      - AdaptivePlan
            //        - InitialPlan
            //          - SomeInitialPlanNode
            //            - RemoteSourceNode(1)
            //        - CurrentPlan
            //          - NewPlanNode
            //            - RemoteSourceNode(1)
            // As shown in the example, the exchange node is replaced with a remote source node in the initial plan.

            AdaptivePlanNode adaptivePlan = (AdaptivePlanNode) context.defaultRewrite(node, context.get());
            List<PlanNode> remoteSourceNodes = getAllRemoteSourceNodes(adaptivePlan.getCurrentPlan(), context.get().getChildren());
            ExchangeNodeToRemoteSourceRewriter rewriter = new ExchangeNodeToRemoteSourceRewriter(remoteSourceNodes, unchangedSubPlans.keySet());
            PlanNode newInitialPlan = SimplePlanRewriter.rewriteWith(rewriter, adaptivePlan.getInitialPlan());
            Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(newInitialPlan);
            return new AdaptivePlanNode(adaptivePlan.getId(), newInitialPlan, dependencies, adaptivePlan.getCurrentPlan());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<FragmentProperties> context)
        {
            List<SubPlan> completedChildren = unchangedSubPlans.values().stream()
                    .filter(subPlan -> node.getSourceFragmentIds().contains(subPlan.getFragment().getId()))
                    .collect(toImmutableList());
            checkState(completedChildren.size() == node.getSourceFragmentIds().size(), "completedSubPlans should contain all remote source children");

            if (node.getExchangeType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (node.getExchangeType() == ExchangeNode.Type.REPARTITION) {
                for (SubPlan child : completedChildren) {
                    PartitioningScheme partitioningScheme = child.getFragment().getOutputPartitioningScheme();
                    context.get().setDistribution(
                            partitioningScheme.getPartitioning().getHandle(),
                            partitioningScheme.getPartitionCount(),
                            metadata,
                            session);
                }
            }
            context.get().addChildren(completedChildren);
            return node;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            if (exchange.getScope() != REMOTE) {
                return context.defaultRewrite(exchange, context.get());
            }

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(
                        partitioningScheme.getPartitioning().getHandle(),
                        partitioningScheme.getPartitionCount(),
                        metadata,
                        session);
            }

            ImmutableList.Builder<FragmentProperties> childrenProperties = ImmutableList.builder();
            ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
                childrenProperties.add(childProperties);
                childrenBuilder.add(buildSubPlan(
                        exchange.getSources().get(sourceIndex),
                        new ExchangeSourceId(exchange.getId(), exchange.getSources().get(sourceIndex).getId()),
                        childProperties,
                        context));
            }

            List<SubPlan> children = childrenBuilder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(
                    exchange.getId(),
                    childrenIds,
                    exchange.getOutputSymbols(),
                    exchange.getOrderingScheme(),
                    exchange.getType(),
                    isWorkerCoordinatorBoundary(context.get(), childrenProperties.build()) ? getRetryPolicy(session) : RetryPolicy.NONE);
        }

        private SubPlan buildSubPlan(PlanNode node, ExchangeSourceId exchangeSourceId, FragmentProperties properties, RewriteContext<FragmentProperties> context)
        {
            SubPlan subPlan = unchangedSubPlans.get(exchangeSourceId);
            if (subPlan != null) {
                return subPlan;
            }
            PlanFragmentId planFragmentId = idAllocator.getNextId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId);
        }

        private List<PlanNode> getAllRemoteSourceNodes(PlanNode node, List<SubPlan> children)
        {
            return Stream.concat(
                            children.stream()
                                    .map(SubPlan::getFragment)
                                    .flatMap(fragment -> fragment.getRemoteSourceNodes().stream()),
                            PlanNodeSearcher.searchFrom(node)
                                    .whereIsInstanceOfAny(RemoteSourceNode.class)
                                    .findAll().stream())
                    .collect(toImmutableList());
        }
    }

    public static class FragmentProperties
    {
        private final Map<PlanFragmentId, SubPlan> children = new LinkedHashMap<>();

        private final Optional<PartitioningScheme> partitioningScheme;
        private final Optional<NewIrPartitioningScheme> newIrPartitioningScheme;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private boolean containsEmptyValues;
        private OptionalInt partitionCount = OptionalInt.empty();
        private final Set<PlanNodeId> partitionedSources = new HashSet<>();

        public FragmentProperties(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = Optional.of(partitioningScheme);
            this.newIrPartitioningScheme = Optional.empty();
        }

        public FragmentProperties(NewIrPartitioningScheme newIrPartitioningScheme)
        {
            this.partitioningScheme = Optional.empty();
            this.newIrPartitioningScheme = Optional.of(newIrPartitioningScheme);
        }

        public List<SubPlan> getChildren()
        {
            return ImmutableList.copyOf(children.values());
        }

        public boolean hasDistribution()
        {
            return partitioningHandle.isPresent();
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(partitioningHandle.isEmpty(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(
                PartitioningHandle distribution,
                OptionalInt partitionCount,
                Metadata metadata,
                Session session)
        {
            if (partitionCount.isPresent()) {
                this.partitionCount = partitionCount;
            }

            if (partitioningHandle.isEmpty()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = this.partitioningHandle.get();

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.isSingleNode()) {
                return this;
            }

            if (isCompatibleSystemPartitioning(distribution)) {
                return this;
            }

            if (isCompatibleScaledWriterPartitioning(currentPartitioning, distribution)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            if (currentPartitioning.equals(SOURCE_DISTRIBUTION)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    this.partitioningHandle));
        }

        private boolean isCompatibleSystemPartitioning(PartitioningHandle distribution)
        {
            ConnectorPartitioningHandle currentHandle = partitioningHandle.get().getConnectorHandle();
            ConnectorPartitioningHandle distributionHandle = distribution.getConnectorHandle();
            if ((currentHandle instanceof SystemPartitioningHandle currentPartitioningHandle) &&
                    (distributionHandle instanceof SystemPartitioningHandle distributionPartitioningHandle)) {
                return currentPartitioningHandle.getPartitioning() == distributionPartitioningHandle.getPartitioning();
            }
            return false;
        }

        private static boolean isCompatibleScaledWriterPartitioning(PartitioningHandle current, PartitioningHandle suggested)
        {
            if (current.equals(FIXED_HASH_DISTRIBUTION) && suggested.equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
                return true;
            }
            PartitioningHandle currentWithScaledWritersEnabled = new PartitioningHandle(
                    current.getCatalogHandle(),
                    current.getTransactionHandle(),
                    current.getConnectorHandle(),
                    true);
            return currentWithScaledWritersEnabled.equals(suggested);
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(partitioningHandle.isEmpty() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution, Metadata metadata, Session session)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);

            if (partitioningHandle.isEmpty()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = partitioningHandle.get();

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.equals(SINGLE_DISTRIBUTION) || currentPartitioning.equals(COORDINATOR_DISTRIBUTION)) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format("Cannot overwrite distribution with %s (currently set to %s)", distribution, currentPartitioning));
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            children.stream()
                    .forEach(child -> this.children.put(child.getFragment().getId(), child));

            return this;
        }

        public void setContainsEmptyValues()
        {
            this.containsEmptyValues = true;
        }

        public PartitioningScheme getPartitioningScheme()
        {
            return partitioningScheme.orElseThrow();
        }

        public NewIrPartitioningScheme getNewIrPartitioningScheme()
        {
            return newIrPartitioningScheme.orElseThrow();
        }

        public PartitioningHandle getPartitioningHandle()
        {
            checkState(partitioningHandle.isPresent() || containsEmptyValues, "PartitioningHandle is not set for a fragment that does not contain empty values");

            return partitioningHandle.orElse(SINGLE_DISTRIBUTION);
        }

        public OptionalInt getPartitionCount()
        {
            return partitionCount;
        }

        public Set<PlanNodeId> getPartitionedSources()
        {
            return partitionedSources;
        }
    }

    private static final class PartitioningHandleReassigner
            extends SimplePlanRewriter<Void>
    {
        private final PartitioningHandle fragmentPartitioningHandle;
        private final Metadata metadata;
        private final Session session;

        public PartitioningHandleReassigner(PartitioningHandle fragmentPartitioningHandle, Metadata metadata, Session session)
        {
            this.fragmentPartitioningHandle = fragmentPartitioningHandle;
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public TableScanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Optional<TablePartitioning> tablePartitioning = metadata.getTableProperties(session, node.getTable()).getTablePartitioning();
            if (tablePartitioning.isEmpty() || !node.isUseConnectorNodePartitioning()) {
                if (!fragmentPartitioningHandle.equals(SOURCE_DISTRIBUTION)) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid query plan: Expected SOURCE_DISTRIBUTION for unpartitioned table scan node, but got " + fragmentPartitioningHandle);
                }
                return node;
            }

            if (tablePartitioning.get().partitioningHandle().equals(fragmentPartitioningHandle)) {
                // do nothing if the current scan node's partitioning matches the fragment's
                return node;
            }

            // The table partitioning is incompatible with the fragment's partitioning, so push partitioning into the table scan.
            // The applyPartitioning is expected to succeed, because the only way to get here is if getCommonPartitioning returned a non-empty value.
            TableHandle newTable = metadata.applyPartitioning(session, node.getTable(), Optional.of(fragmentPartitioningHandle), tablePartitioning.get().partitioningColumns())
                    .orElseThrow(() -> new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid query plan: Table partitioning not compatible with fragment partitioning"));
            return new TableScanNode(
                    node.getId(),
                    newTable,
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    // plan was already fragmented with scan node's partitioning
                    // and new partitioning is compatible with previous one
                    Optional.of(true));
        }

        @Override
        public PlanNode visitChooseAlternativeNode(ChooseAlternativeNode node, RewriteContext<Void> context)
        {
            List<PlanNode> newAlternatives = node.getSources().stream()
                    .map(alternative -> context.defaultRewrite(alternative, context.get()))
                    .toList();
            TableScanNode newTableScan = visitTableScan(node.getOriginalTableScan().tableScanNode(), context);
            FilteredTableScan newFilteredTableScan = new FilteredTableScan(newTableScan, node.getOriginalTableScan().filterPredicate());
            return new ChooseAlternativeNode(node.getId(), newAlternatives, newFilteredTableScan);
        }
    }

    private static final class ExchangeNodeToRemoteSourceRewriter
            extends SimplePlanRewriter<Void>
    {
        private final List<PlanNode> remoteSourceNodes;
        private final Set<ExchangeSourceId> unchangedRemoteExchanges;

        public ExchangeNodeToRemoteSourceRewriter(List<PlanNode> remoteSourceNodes, Set<ExchangeSourceId> unchangedRemoteExchanges)
        {
            this.remoteSourceNodes = requireNonNull(remoteSourceNodes, "remoteSourceNodes is null");
            this.unchangedRemoteExchanges = requireNonNull(unchangedRemoteExchanges, "unchangedRemoteExchanges is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            if (node.getScope() != REMOTE || !isUnchangedFragment(node.getId())) {
                return context.defaultRewrite(node, context.get());
            }
            return remoteSourceNodes.stream()
                    .filter(remoteSource -> remoteSource.getId().equals(node.getId()))
                    .findFirst()
                    .orElse(node);
        }

        private boolean isUnchangedFragment(PlanNodeId exchangeID)
        {
            return unchangedRemoteExchanges.stream().anyMatch(fragment -> fragment.exchangeId().equals(exchangeID));
        }
    }
}
