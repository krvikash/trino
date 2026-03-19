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
package io.trino.sql.planner.newirtoold;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.LanguageFunctionProvider.LanguageFunctionData;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.operator.RetryPolicy;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.function.FunctionId;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.AssignUniqueId;
import io.trino.sql.dialect.trino.operation.CorrelatedJoin;
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.dialect.trino.operation.EnforceSingleRow;
import io.trino.sql.dialect.trino.operation.Except;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.ExplainAnalyze;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.GroupId;
import io.trino.sql.dialect.trino.operation.Intersect;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Sort;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TopN;
import io.trino.sql.dialect.trino.operation.TopNRanking;
import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.dialect.trino.operation.TrinoOperationVisitor;
import io.trino.sql.dialect.trino.operation.Union;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operation.Window;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmentIdAllocator;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanFragmenter.FragmentProperties;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.optimizations.ctereuse.FieldMapping;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isForceSingleNodeOutput;
import static io.trino.sql.dialect.trino.ProgramBuilder.initializeNameAllocator;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.BUCKET_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.BUCKET_TO_PARTITION;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.EXCHANGE_TYPE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeType.GATHER;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeType.REPARTITION;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.PARTITION_COUNT;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.REPLICATE_NULLS_AND_ANY;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata.CARDINALITY;
import static io.trino.sql.planner.PlanFragmenter.isWorkerCoordinatorBoundary;
import static io.trino.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.newirtoold.ToOldIrRelationalRewriter.getPartitioning;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getInversedMapping;
import static io.trino.sql.planner.optimizations.ctereuse.RewriteUtils.rebaseBlock;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonFragmentPlan;
import static java.util.Objects.requireNonNull;

/**
 * Plan fragmenter for the new IR, based on {@link PlanFragmenter}.
 * It fragments the given {@link Program} (a query plan in new IR), and returns a structure of {@link SubPlan}s, similarly  to PlanFragmenter.
 * SubPlans are based on the old IR, and therefore the fragmentation result is compatible with downstream query processing.
 * <p>
 * Note: The given Program potentially involves diamond shape, which is indicated by a remote exchange operation being referenced multiple times.
 * In such a case, the fragmentation result has multiple downstream plan fragments depending on a single upstream plan fragment.
 * In the fragmented plan, the diamond shape manifests itself as multiple references to te same PlanFragmentId in different RemoteSourceNodes.
 */
public class NewIrFragmenter
{
    private final Session session;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final List<CatalogProperties> activeCatalogs;
    private final Map<FunctionId, LanguageFunctionData> languageFunctions;
    private final PlanFragmentIdAllocator planFragmentIdAllocator;
    private final PlanNodeIdAllocator planNodeIdAllocator;
    private final SymbolAllocator symbolAllocator;

    public NewIrFragmenter(
            Session session,
            Metadata metadata,
            FunctionManager functionManager,
            List<CatalogProperties> activeCatalogs,
            Map<FunctionId, LanguageFunctionData> languageFunctions)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
        this.languageFunctions = ImmutableMap.copyOf(languageFunctions);
        this.planFragmentIdAllocator = new PlanFragmentIdAllocator(0);
        this.planNodeIdAllocator = new PlanNodeIdAllocator();
        this.symbolAllocator = new SymbolAllocator();
    }

    public SubPlan fragmentProgram(Program program, FragmentProperties rootProperties)
    {
        Block mainBlock = ((Query) program.root()).query();

        // build the result-to-operation map for top-level relational operations
        Map<Value, TrinoOperation> resultToOperation = mainBlock.operations().stream()
                .collect(toImmutableMap(Operation::result, TrinoOperation.class::cast));

        Fragmenter fragmenter = new Fragmenter(
                resultToOperation,
                session,
                metadata,
                functionManager,
                activeCatalogs,
                languageFunctions,
                planFragmentIdAllocator,
                planNodeIdAllocator,
                symbolAllocator,
                initializeNameAllocator(program));

        PlanNode root = ((Output) mainBlock.getTerminalOperation()).accept(fragmenter, rootProperties);

        return fragmenter.buildRootFragment(root, rootProperties);
    }

    private static class Fragmenter
            extends TrinoOperationVisitor<PlanNode, FragmentProperties>
    {
        private final Map<Value, TrinoOperation> resultToOperation;
        private final Session session;
        private final Metadata metadata;
        private final FunctionManager functionManager;
        private final List<CatalogProperties> activeCatalogs;
        private final Map<FunctionId, LanguageFunctionData> languageFunctions;
        private final PlanFragmentIdAllocator planFragmentIdAllocator;
        private final PlanFragmentId rootFragmentId;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final SymbolAllocator symbolAllocator;
        private final ProgramBuilder.ValueNameAllocator nameAllocator;
        private final ToOldIrScalarRewriter scalarRewriter;
        private final ToOldIrRelationalRewriter relationalRewriter;
        // the given plan potentially involves diamond shape, indicated by a remote exchange operation being referenced multiple times.
        // a remote exchange is rewritten into RemoteSourceNode. This is a map of original exchange operation result to the created RemoteSourceNode
        // and its children. It helps us to avoid repetition and reuse the upstream plan.
        private final Map<Value, ProcessedRemoteExchange> processedRemoteExchanges;

        public Fragmenter(
                Map<Value, TrinoOperation> resultToOperation,
                Session session,
                Metadata metadata,
                FunctionManager functionManager,
                List<CatalogProperties> activeCatalogs,
                Map<FunctionId, LanguageFunctionData> languageFunctions,
                PlanFragmentIdAllocator planFragmentIdAllocator,
                PlanNodeIdAllocator planNodeIdAllocator,
                SymbolAllocator symbolAllocator,
                ProgramBuilder.ValueNameAllocator nameAllocator)
        {
            this.resultToOperation = ImmutableMap.copyOf(resultToOperation);
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
            this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
            this.languageFunctions = ImmutableMap.copyOf(languageFunctions);
            this.planFragmentIdAllocator = requireNonNull(planFragmentIdAllocator, "planFragmentIdAllocator is null");
            this.rootFragmentId = planFragmentIdAllocator.getNextId();
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.nameAllocator = requireNonNull(nameAllocator, "nameAllocator is null");
            this.scalarRewriter = new ToOldIrScalarRewriter(symbolAllocator);
            this.relationalRewriter = new ToOldIrRelationalRewriter(planNodeIdAllocator, symbolAllocator, scalarRewriter, session, metadata);
            this.processedRemoteExchanges = new HashMap<>();
        }

        @Override
        protected PlanNode visitOperation(TrinoOperation operation, FragmentProperties context)
        {
            throw new UnsupportedOperationException("Fragmenter is not implemented for " + operation.name() + ". It must support all relational operations.");
        }

        @Override
        public PlanNode visitAggregation(Aggregation operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitCorrelatedJoin(CorrelatedJoin operation, FragmentProperties context)
        {
            throw new IllegalStateException("Unexpected operation: " + operation.name());
        }

        @Override
        public PlanNode visitDynamicFilterSource(DynamicFilterSource operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitEnforceSingleRow(EnforceSingleRow operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitExcept(Except operation, FragmentProperties context)
        {
            List<PlanNode> rewrittenSources = getSources(operation).stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            return operation.accept(relationalRewriter, rewrittenSources);
        }

        @Override
        public PlanNode visitExchange(Exchange operation, FragmentProperties context)
        {
            if (EXCHANGE_SCOPE.getAttribute(operation.attributes()) != REMOTE) {
                List<PlanNode> rewrittenSources = getSources(operation).stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                return operation.accept(relationalRewriter, rewrittenSources);
            }

            NewIrPartitioningScheme partitioningScheme = NewIrPartitioningScheme.of(operation, nameAllocator);

            if (EXCHANGE_TYPE.getAttribute(operation.attributes()) == GATHER) {
                context.setSingleNodeDistribution();
            }
            else if (EXCHANGE_TYPE.getAttribute(operation.attributes()) == REPARTITION) {
                context.setDistribution(
                        partitioningScheme.handle(),
                        partitioningScheme.partitionCount(),
                        metadata,
                        session);
            }

            // check if the exchange has been already visited. It might have been visited by another branch because of diamond shape.
            ProcessedRemoteExchange processedRemoteExchange = processedRemoteExchanges.get(operation.result());
            if (processedRemoteExchange != null) {
                // make sure that context is properly updated. Distribution has been updated above. We need to report the children.
                // Note: in case when we're visiting the same remote exchange again in the same fragment, children have been already added to the context.
                // children are deduplicated in the `addChildren()` method.
                context.addChildren(processedRemoteExchange.children());

                // we can return the previously created RemoteSourceNode. We must clone it for disambiguation, and recalculate the retry policy.
                RetryPolicy newRetryPolicy = isWorkerCoordinatorBoundary(context, processedRemoteExchange.childrenProperties()) ? getRetryPolicy(session) : RetryPolicy.NONE;
                return clone(processedRemoteExchange.remoteSourceNode(), newRetryPolicy);
            }

            ImmutableList.Builder<FragmentProperties> childrenPropertiesBuilder = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> childrenNodesBuilder = ImmutableList.builder();
            ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
            List<TrinoOperation> sources = getSources(operation);
            for (int sourceIndex = 0; sourceIndex < sources.size(); sourceIndex++) {
                TrinoOperation source = sources.get(sourceIndex);

                // rebase the partitioning scheme onto the source: must rebase the ^boundArguments block onto the source relation row type
                // 1. get mapping for input field selector of the source, and inverse it
                FieldMapping mapping = getInversedMapping(operation.inputFieldSelectors().get(sourceIndex));
                // 2. rebase the ^boundArguments block
                Block newBoundArguments = rebaseBlock(partitioningScheme.partitioningBoundArguments(), relationRowType(trinoType(source.result().type())), mapping, nameAllocator).orElseThrow();
                // 3. build the rebased partitioning scheme
                NewIrPartitioningScheme newPartitioningScheme = new NewIrPartitioningScheme(
                        operation.inputFieldSelectors().get(sourceIndex).withLabel("^outputLayoutSelector"),
                        partitioningScheme.handle(),
                        newBoundArguments,
                        partitioningScheme.partitioningReplicateNullsAndAny(),
                        partitioningScheme.partitioningBucketToPartition(),
                        partitioningScheme.bucketCount(),
                        partitioningScheme.partitionCount());

                FragmentProperties childProperties = new FragmentProperties(newPartitioningScheme);
                childrenPropertiesBuilder.add(childProperties);
                PlanFragmentId planFragmentId = planFragmentIdAllocator.getNextId();
                PlanNode child = source.accept(this, childProperties);
                childrenNodesBuilder.add(child);
                childrenBuilder.add(buildFragment(child, childProperties, planFragmentId));
            }

            List<SubPlan> children = childrenBuilder.build();
            context.addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            ExchangeNode exchangeNode = (ExchangeNode) operation.accept(relationalRewriter, childrenNodesBuilder.build());

            List<FragmentProperties> childrenProperties = childrenPropertiesBuilder.build();

            RemoteSourceNode remoteSourceNode = new RemoteSourceNode(
                    exchangeNode.getId(),
                    childrenIds,
                    exchangeNode.getOutputSymbols(),
                    exchangeNode.getOrderingScheme(),
                    exchangeNode.getType(),
                    isWorkerCoordinatorBoundary(context, childrenProperties) ? getRetryPolicy(session) : RetryPolicy.NONE);

            processedRemoteExchanges.put(operation.result(), new ProcessedRemoteExchange(remoteSourceNode, children, childrenProperties));
            return remoteSourceNode;
        }

        /**
         * Copy the RemoteSourceNode, reallocate all symbols and assign a new PlanNodeId, so that the original RemoteSourceNode,
         * and the cloned one can be used in the same context without ambiguity.
         */
        private RemoteSourceNode clone(RemoteSourceNode remoteSourceNode, RetryPolicy newRetryPolicy)
        {
            Map<Symbol, Symbol> mapping = new HashMap<>();
            remoteSourceNode.getOutputSymbols().stream()
                    .forEach(symbol -> mapping.put(symbol, symbolAllocator.newSymbol(symbol)));
            List<Symbol> newOutputs = remoteSourceNode.getOutputSymbols().stream()
                    .map(mapping::get)
                    .collect(toImmutableList());
            Optional<OrderingScheme> newOrderingScheme = remoteSourceNode.getOrderingScheme()
                    .map(orderingScheme -> new OrderingScheme(
                            orderingScheme.orderBy().stream()
                                    .map(mapping::get)
                                    .collect(toImmutableList()),
                            orderingScheme.orderings().entrySet().stream()
                                    .collect(toImmutableMap(entry -> mapping.get(entry.getKey()), Map.Entry::getValue))));

            return new RemoteSourceNode(
                    planNodeIdAllocator.getNextId(),
                    remoteSourceNode.getSourceFragmentIds(),
                    newOutputs,
                    newOrderingScheme,
                    remoteSourceNode.getExchangeType(),
                    newRetryPolicy);
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyze operation, FragmentProperties context)
        {
            context.setCoordinatorOnlyDistribution();
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitFilter(Filter operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitGroupId(GroupId operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitIntersect(Intersect operation, FragmentProperties context)
        {
            List<PlanNode> rewrittenSources = getSources(operation).stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            return operation.accept(relationalRewriter, rewrittenSources);
        }

        @Override
        public PlanNode visitJoin(Join operation, FragmentProperties context)
        {
            List<TrinoOperation> sources = getSources(operation);
            checkState(sources.size() == 2, "Expected two sources for %s", operation.name());
            List<PlanNode> rewrittenSources = sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            return operation.accept(relationalRewriter, rewrittenSources);
        }

        @Override
        public PlanNode visitLimit(Limit operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitOutput(Output operation, FragmentProperties context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.setSingleNodeDistribution();
            }
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitProject(Project operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitSort(Sort operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitTableScan(TableScan operation, FragmentProperties context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, TABLE_HANDLE.getAttribute(operation.attributes()))
                    .getTablePartitioning()
                    .filter(value -> Optional.ofNullable(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(operation.attributes()))
                            .orElseThrow(() -> new VerifyException(USE_CONNECTOR_NODE_PARTITIONING.name() + " attribute is not set")))
                    .map(TablePartitioning::partitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            TableScanNode tableScanNode = (TableScanNode) operation.accept(relationalRewriter, ImmutableList.of());
            context.addSourceDistribution(tableScanNode.getId(), partitioning, metadata, session);
            return tableScanNode;
        }

        @Override
        public PlanNode visitTopN(TopN operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitTopNRanking(TopNRanking operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitUnion(Union operation, FragmentProperties context)
        {
            List<PlanNode> rewrittenSources = getSources(operation).stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
            return operation.accept(relationalRewriter, rewrittenSources);
        }

        @Override
        public PlanNode visitValues(Values operation, FragmentProperties context)
        {
            // An empty values node is compatible with any distribution, so
            // don't attempt to overwrite the one that's already been chosen
            if (CARDINALITY.getAttribute(operation.attributes()) != 0) {
                context.setSingleNodeDistribution();
            }
            else {
                // An empty values node is compatible with any distribution, so
                // do not overwrite a distribution if there is one already chosen,
                // and delay setting the distribution in case the fragment contains
                // another node with specific distribution requirements
                context.setContainsEmptyValues();
            }
            return operation.accept(relationalRewriter, ImmutableList.of());
        }

        @Override
        public PlanNode visitWindow(Window operation, FragmentProperties context)
        {
            TrinoOperation source = getSource(operation);
            PlanNode rewrittenSource = source.accept(this, context);
            return operation.accept(relationalRewriter, ImmutableList.of(rewrittenSource));
        }

        private TrinoOperation getSource(TrinoOperation operation)
        {
            return resultToOperation.get(getOnlyElement(operation.arguments()));
        }

        private List<TrinoOperation> getSources(TrinoOperation operation)
        {
            return operation.arguments().stream()
                    .map(resultToOperation::get)
                    .collect(toImmutableList());
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties rootProperties)
        {
            return buildFragment(root, rootProperties, rootFragmentId);
        }

        public SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(root);

            // scheduleOrder traverses the fragment and collects all leaf nodes.
            // It could seem that with diamond-shaped plans, we need to make sure we don't collect the same node multiple times.
            // However, in the fragmented plan, the diamond shape manifests itself as multiple references to the same PlanFragmentId
            // in different RemoteSourceNodes. The fragments do not contain the diamond shape internally.
            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            // rewrite the NewIrPartitioningScheme to old IR. Translate the three blocks: ^outputLayoutSelector and ^boundArguments to old IR using the output symbols of root.
            NewIrPartitioningScheme newIrPartitioningScheme = properties.getNewIrPartitioningScheme();
            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    getPartitioning(newIrPartitioningScheme.handle(), relationalRewriter.getBoundArguments(newIrPartitioningScheme.partitioningBoundArguments(), root.getOutputSymbols())),
                    scalarRewriter.getSelectedSymbols(newIrPartitioningScheme.outputLayoutSelector(), root.getOutputSymbols()),
                    newIrPartitioningScheme.partitioningReplicateNullsAndAny(),
                    newIrPartitioningScheme.partitioningBucketToPartition().map(list -> list.stream().mapToInt(Integer::intValue).toArray()),
                    newIrPartitioningScheme.bucketCount(),
                    newIrPartitioningScheme.partitionCount());

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    dependencies,
                    properties.getPartitioningHandle(),
                    properties.getPartitionCount(),
                    schedulingOrder,
                    partitioningScheme,
                    OptionalInt.empty(),
                    StatsAndCosts.empty(),
                    activeCatalogs,
                    languageFunctions,
                    Optional.of(jsonFragmentPlan(root, metadata, functionManager, session)));

            return new SubPlan(fragment, properties.getChildren());
        }
    }

    /**
     * PartitioningScheme representation for the new IR, based on {@link PartitioningScheme}.
     */
    public record NewIrPartitioningScheme(
            Block outputLayoutSelector,
            PartitioningHandle handle,
            Block partitioningBoundArguments,
            boolean partitioningReplicateNullsAndAny,
            Optional<List<Integer>> partitioningBucketToPartition,
            OptionalInt bucketCount,
            OptionalInt partitionCount)
    {
        public NewIrPartitioningScheme
        {
            requireNonNull(outputLayoutSelector, "outputLayoutSelector is null");
            requireNonNull(handle, "handle is null");
            requireNonNull(partitioningBoundArguments, "partitioningBoundArguments is null");
            partitioningBucketToPartition = partitioningBucketToPartition.map(ImmutableList::copyOf);
            requireNonNull(bucketCount, "bucketCount is null");
            requireNonNull(partitionCount, "partitionCount is null");
            // TODO validate blocks: outputLayoutSelector, partitioningBoundArguments
        }

        public static NewIrPartitioningScheme of(Exchange exchange, ProgramBuilder.ValueNameAllocator nameAllocator)
        {
            Block partitioningBoundArguments = exchange.partitioningBoundArguments();
            Block outputLayoutSelector = getFullPassthroughFieldSelector(
                    "^outputLayoutSelector",
                    trinoType(getOnlyElement(partitioningBoundArguments.parameters()).type()),
                    nameAllocator);

            return new NewIrPartitioningScheme(
                    outputLayoutSelector,
                    PARTITIONING_HANDLE.getAttribute(exchange.attributes()),
                    partitioningBoundArguments,
                    REPLICATE_NULLS_AND_ANY.getAttribute(exchange.attributes()),
                    Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(exchange.attributes())),
                    BUCKET_COUNT.getAttribute(exchange.attributes()) == null ? OptionalInt.empty() : OptionalInt.of(BUCKET_COUNT.getAttribute(exchange.attributes())),
                    PARTITION_COUNT.getAttribute(exchange.attributes()) == null ? OptionalInt.empty() : OptionalInt.of(PARTITION_COUNT.getAttribute(exchange.attributes())));
        }
    }

    /**
     * The structure to record a processed part of the plan. We can reuse the processed result
     * when we visit the same part again, as a result of diamond shape.
     */
    private record ProcessedRemoteExchange(RemoteSourceNode remoteSourceNode, List<SubPlan> children, List<FragmentProperties> childrenProperties)
    {
        public ProcessedRemoteExchange
        {
            requireNonNull(remoteSourceNode, "remoteSourceNode is null");
            children = ImmutableList.copyOf(children);
            childrenProperties = ImmutableList.copyOf(childrenProperties);
            checkArgument(children.size() == childrenProperties.size(), "children and childrenProperties do not match in size");
        }
    }
}
