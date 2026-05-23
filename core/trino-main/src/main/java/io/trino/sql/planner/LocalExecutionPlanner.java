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

import ai.rapids.cudf.DType;
import ai.rapids.cudf.ast.AstExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.base.VerifyException;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cache.CacheDataOperator.CacheDataOperatorFactory;
import io.trino.cache.CacheDriverFactory;
import io.trino.cache.CacheManagerRegistry;
import io.trino.cache.CachePerformanceTracker;
import io.trino.cache.CacheStats;
import io.trino.cache.CommonPlanAdaptation.PlanSignatureWithPredicate;
import io.trino.cache.LoadCachedDataOperator.LoadCachedDataOperatorFactory;
import io.trino.cache.NonEvictableCache;
import io.trino.cache.StaticDynamicFilter;
import io.trino.exchange.ExchangeEncryptionKey;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.StageId;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.operator.AggregationOperator.AggregationOperatorFactory;
import io.trino.operator.AlternativesAwareDriverFactory;
import io.trino.operator.AssignUniqueIdOperator;
import io.trino.operator.DevNullOperator.DevNullOperatorFactory;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.DriverFactory;
import io.trino.operator.DynamicFilterSourceOperator;
import io.trino.operator.DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory;
import io.trino.operator.EnforceSingleRowOperator;
import io.trino.operator.ExchangeOperator.ExchangeOperatorFactory;
import io.trino.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupIdOperator;
import io.trino.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import io.trino.operator.HashSemiJoinOperator;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.LeafTableFunctionOperator.LeafTableFunctionOperatorFactory;
import io.trino.operator.LimitOperator.LimitOperatorFactory;
import io.trino.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import io.trino.operator.MergeOperator.MergeOperatorFactory;
import io.trino.operator.MergeProcessorOperator;
import io.trino.operator.MergeWriterOperator.MergeWriterOperatorFactory;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.OperatorFactory;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.operator.OutputFactory;
import io.trino.operator.OutputSpoolingOperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesSpatialIndexFactory;
import io.trino.operator.PartitionFunction;
import io.trino.operator.RefreshMaterializedViewOperator.RefreshMaterializedViewOperatorFactory;
import io.trino.operator.RowNumberOperator;
import io.trino.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import io.trino.operator.SetBuilderOperator.SetSupplier;
import io.trino.operator.SimpleTableExecuteOperator.SimpleTableExecuteOperatorOperatorFactory;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import io.trino.operator.SplitDriverFactory;
import io.trino.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import io.trino.operator.StreamingAggregationOperator;
import io.trino.operator.TableMutationOperator.TableMutationOperatorFactory;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.TopNOperator;
import io.trino.operator.TopNRankingOperator;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.operator.WindowFunctionDefinition;
import io.trino.operator.WindowOperator.WindowOperatorFactory;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.DistinctAccumulatorFactory;
import io.trino.operator.aggregation.DistinctWindowAccumulator;
import io.trino.operator.aggregation.OrderedAccumulatorFactory;
import io.trino.operator.aggregation.OrderedWindowAccumulator;
import io.trino.operator.aggregation.partial.PartialAggregationController;
import io.trino.operator.exchange.LocalExchange;
import io.trino.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.trino.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.trino.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import io.trino.operator.exchange.PageChannelSelector;
import io.trino.operator.function.RegularTableFunctionPartition.PassThroughColumnSpecification;
import io.trino.operator.function.TableFunctionOperator.TableFunctionOperatorFactory;
import io.trino.operator.gpu.GpuFilter;
import io.trino.operator.gpu.GpuGroupId;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperator;
import io.trino.operator.gpu.GpuProject;
import io.trino.operator.gpu.GpuTopN;
import io.trino.operator.gpu.SentinelSinkOperator;
import io.trino.operator.gpu.aggregation.GpuAggregationCompiler;
import io.trino.operator.gpu.exchange.GpuLocalExchange;
import io.trino.operator.gpu.exchange.GpuLocalExchangeWriter;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.gpu.expression.NodeGpuExecutionEnabled;
import io.trino.operator.gpu.join.CudfAstExpression;
import io.trino.operator.gpu.join.GpuDynamicFilterCollector;
import io.trino.operator.gpu.join.GpuJoinBridgeManager;
import io.trino.operator.gpu.join.GpuJoinBuild;
import io.trino.operator.gpu.join.GpuJoinFilterCompiler;
import io.trino.operator.gpu.join.GpuLookupJoin;
import io.trino.operator.gpu.join.GpuSemiJoin;
import io.trino.operator.gpu.join.GpuSemiJoinBuild;
import io.trino.operator.gpu.join.GpuSemiJoinSetSupplier;
import io.trino.operator.index.DynamicTupleFilterFactory;
import io.trino.operator.index.FieldSetFilteringRecordSet;
import io.trino.operator.index.IndexBuildDriverFactoryProvider;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.index.IndexLookupSourceFactory;
import io.trino.operator.index.IndexManager;
import io.trino.operator.index.IndexSourceOperator;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinOperatorFactory;
import io.trino.operator.join.LookupSourceFactory;
import io.trino.operator.join.NestedLoopJoinBridge;
import io.trino.operator.join.NestedLoopJoinPagesSupplier;
import io.trino.operator.join.nonspilling.HashBuilderOperator;
import io.trino.operator.join.spilling.HashBuilderOperator.HashBuilderOperatorFactory;
import io.trino.operator.join.spilling.PartitionedLookupSourceFactory;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputFactory;
import io.trino.operator.output.PositionsAppenderFactory;
import io.trino.operator.output.SkewedPartitionRebalancer;
import io.trino.operator.output.TaskOutputOperator.TaskOutputFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.operator.unnest.UnnestOperator;
import io.trino.operator.window.AggregateWindowFunction;
import io.trino.operator.window.AggregationWindowFunctionSupplier;
import io.trino.operator.window.FrameInfo;
import io.trino.operator.window.PartitionerSupplier;
import io.trino.operator.window.PatternRecognitionPartitionerSupplier;
import io.trino.operator.window.RegularPartitionerSupplier;
import io.trino.operator.window.matcher.IrRowPatternToProgramRewriter;
import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.matcher.Program;
import io.trino.operator.window.pattern.ArgumentComputation.ArgumentComputationSupplier;
import io.trino.operator.window.pattern.LabelEvaluator.EvaluationSupplier;
import io.trino.operator.window.pattern.LogicalIndexNavigation;
import io.trino.operator.window.pattern.MatchAggregation.MatchAggregationInstantiator;
import io.trino.operator.window.pattern.MatchAggregationPointer;
import io.trino.operator.window.pattern.MeasureComputation.MeasureComputationSupplier;
import io.trino.operator.window.pattern.PhysicalValueAccessor;
import io.trino.operator.window.pattern.PhysicalValuePointer;
import io.trino.operator.window.pattern.SetEvaluator.SetEvaluatorSupplier;
import io.trino.plugin.base.MappedRecordSet;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.QueryDataEncoders;
import io.trino.spi.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.split.AlternativeChooser;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.planner.optimizations.IndexJoinOptimizer;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CacheDataPlanNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.type.BlockTypeOperators;
import io.trino.type.FunctionType;
import org.objectweb.asm.MethodTooLargeException;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static io.trino.SystemSessionProperties.getAdaptivePartialAggregationUniqueRowsRatioThreshold;
import static io.trino.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static io.trino.SystemSessionProperties.getCacheMaxSplitSize;
import static io.trino.SystemSessionProperties.getDynamicRowFilterSelectivityThreshold;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static io.trino.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static io.trino.SystemSessionProperties.getPagePartitioningBufferPoolSize;
import static io.trino.SystemSessionProperties.getSkewedPartitionMinDataProcessedRebalanceThreshold;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.getTaskMaxWriterCount;
import static io.trino.SystemSessionProperties.getTaskMinWriterCount;
import static io.trino.SystemSessionProperties.getWriterScalingMinDataProcessed;
import static io.trino.SystemSessionProperties.isAdaptiveFilterReorderingEnabled;
import static io.trino.SystemSessionProperties.isAdaptivePartialAggregationEnabled;
import static io.trino.SystemSessionProperties.isColumnarFilterEvaluationEnabled;
import static io.trino.SystemSessionProperties.isDebugOutputEnabled;
import static io.trino.SystemSessionProperties.isEnableDynamicRowFiltering;
import static io.trino.SystemSessionProperties.isForceSpillingOperator;
import static io.trino.SystemSessionProperties.isParallelizeLookupOuterOperator;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.SystemSessionProperties.isUseCardinalityBasedPartialAggregationController;
import static io.trino.cache.CacheCommonSubqueries.getLoadCachedDataPlanNode;
import static io.trino.cache.CacheCommonSubqueries.isCacheChooseAlternativeNode;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilter;
import static io.trino.cache.StaticDynamicFilter.createStaticDynamicFilterSupplier;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.execution.buffer.PagesSerdes.createExchangePagesSerdeFactory;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static io.trino.operator.HashArraySizeSupplier.incrementalLoadFactorHashArraySizeSupplier;
import static io.trino.operator.OperatorFactories.join;
import static io.trino.operator.OperatorFactories.spillingJoin;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.TableFinishOperator.TableFinishOperatorFactory;
import static io.trino.operator.TableFinishOperator.TableFinisher;
import static io.trino.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.trino.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.trino.operator.TableWriterOperator.STATS_START_CHANNEL;
import static io.trino.operator.TableWriterOperator.TableWriterOperatorFactory;
import static io.trino.operator.WindowFunctionDefinition.window;
import static io.trino.operator.aggregation.AccumulatorCompiler.generateAccumulatorFactory;
import static io.trino.operator.join.JoinUtils.isBuildSideReplicated;
import static io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static io.trino.operator.join.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static io.trino.operator.output.SkewedPartitionRebalancer.createPartitionFunction;
import static io.trino.operator.output.SkewedPartitionRebalancer.getMaxWritersBasedOnMemory;
import static io.trino.operator.output.SkewedPartitionRebalancer.getTaskCount;
import static io.trino.operator.window.FrameInfo.Ordering.ASCENDING;
import static io.trino.operator.window.FrameInfo.Ordering.DESCENDING;
import static io.trino.operator.window.pattern.PhysicalValuePointer.CLASSIFIER;
import static io.trino.operator.window.pattern.PhysicalValuePointer.MATCH_NUMBER;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.spi.gpu.GpuTypeConversion.toDTypes;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.planner.ExpressionExtractor.extractExpressions;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.SortExpressionExtractor.extractSortExpression;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.util.MoreLists.mappedCopy;
import static io.trino.util.MoreMath.previousPowerOfTwo;
import static io.trino.util.SpatialJoinUtils.ST_CONTAINS;
import static io.trino.util.SpatialJoinUtils.ST_DISTANCE;
import static io.trino.util.SpatialJoinUtils.ST_INTERSECTS;
import static io.trino.util.SpatialJoinUtils.ST_WITHIN;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static io.trino.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    private final PageSourceManager pageSourceManager;
    private final CacheManagerRegistry cacheManagerRegistry;
    private final CachePerformanceTracker cachePerformanceTracker;
    private final JsonCodec<TupleDomain> tupleDomainCodec;
    private final AlternativeChooser alternativeChooser;
    private final IndexManager indexManager;
    private final PartitionFunctionProvider partitionFunctionProvider;
    private final PageSinkManager pageSinkManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final ExpressionCompiler expressionCompiler;
    private final boolean nodeGpuExecutionEnabled;
    private final GpuExpressionCompiler gpuExpressionCompiler;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;
    private final CacheStats cacheStats;
    private final DataSize maxPartialAggregationMemorySize;
    private final DataSize maxPagePartitioningBufferSize;
    private final DataSize maxLocalExchangeBufferSize;
    private final DataSize gpuLocalExchangeBufferSize;
    private final SpillerFactory spillerFactory;
    private final QueryDataEncoders encoders;
    private final Optional<SpoolingManager> spoolingManager;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final FlatHashStrategyCompiler hashStrategyCompiler;
    private final OrderingCompiler orderingCompiler;
    private final int maxDistinctValuesPerDriver;
    private final int partitionedMaxDistinctValuesPerDriver;
    private final int bloomFilterMaxDistinctValuesPerDriver;
    private final int partitionedBloomFilterMaxDistinctValuesPerDriver;
    private final DataSize maxSizePerDriver;
    private final DataSize partitionedMaxSizePerDriver;
    private final DataSize maxSizePerOperator;
    private final DataSize partitionedMaxSizePerOperator;
    private final BlockTypeOperators blockTypeOperators;
    private final TypeOperators typeOperators;
    private final NullSafeHashCompiler hashCompiler;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final PositionsAppenderFactory positionsAppenderFactory;
    private final NodeVersion version;
    private final int maxMethodComplexity;
    private final boolean specializeAggregationLoops;
    private final boolean columnarFilterSubexpressionEvaluationEnabled;

    private final NonEvictableCache<FunctionKey, AccumulatorFactory> accumulatorFactoryCache = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, HOURS));
    private final NonEvictableCache<FunctionKey, AggregationWindowFunctionSupplier> aggregationWindowFunctionSupplierCache = buildNonEvictableCache(CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(1, HOURS));

    @Inject
    public LocalExecutionPlanner(
            PlannerContext plannerContext,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceManager pageSourceManager,
            AlternativeChooser alternativeChooser,
            IndexManager indexManager,
            PartitionFunctionProvider partitionFunctionProvider,
            PageSinkManager pageSinkManager,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            @NodeGpuExecutionEnabled boolean nodeGpuExecutionEnabled,
            GpuExpressionCompiler gpuExpressionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            CacheStats cacheStats,
            TaskManagerConfig taskManagerConfig,
            SpillerFactory spillerFactory,
            QueryDataEncoders encoders,
            Optional<SpoolingManager> spoolingManager,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            FlatHashStrategyCompiler hashStrategyCompiler,
            OrderingCompiler orderingCompiler,
            DynamicFilterConfig dynamicFilterConfig,
            BlockTypeOperators blockTypeOperators,
            TypeOperators typeOperators,
            NullSafeHashCompiler hashCompiler,
            TableExecuteContextManager tableExecuteContextManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            CacheManagerRegistry cacheManagerRegistry,
            CachePerformanceTracker cachePerformanceTracker,
            JsonCodec<TupleDomain> tupleDomainCodec,
            NodeVersion version,
            CompilerConfig compilerConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceManager = requireNonNull(pageSourceManager, "pageSourceManager is null");
        this.alternativeChooser = requireNonNull(alternativeChooser, "alternativeChooser is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.partitionFunctionProvider = requireNonNull(partitionFunctionProvider, "partitionFunctionProvider is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "expressionCompiler is null");
        this.nodeGpuExecutionEnabled = nodeGpuExecutionEnabled;
        this.gpuExpressionCompiler = requireNonNull(gpuExpressionCompiler, "gpuExpressionCompiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "joinFilterFunctionCompiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.cacheStats = requireNonNull(cacheStats, "cacheStats is null");
        this.maxIndexMemorySize = taskManagerConfig.getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.encoders = requireNonNull(encoders, "encoders is null");
        this.spoolingManager = requireNonNull(spoolingManager, "spoolingManager is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.gpuLocalExchangeBufferSize = taskManagerConfig.getGpuLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.hashStrategyCompiler = requireNonNull(hashStrategyCompiler, "hashStrategyCompiler is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.maxDistinctValuesPerDriver = dynamicFilterConfig.getMaxDistinctValuesPerDriver();
        this.maxSizePerDriver = dynamicFilterConfig.getMaxSizePerDriver();
        this.partitionedMaxSizePerDriver = dynamicFilterConfig.getPartitionedMaxSizePerDriver();
        this.maxSizePerOperator = dynamicFilterConfig.getMaxSizePerOperator();
        this.partitionedMaxSizePerOperator = dynamicFilterConfig.getPartitionedMaxSizePerOperator();
        this.partitionedMaxDistinctValuesPerDriver = dynamicFilterConfig.getPartitionedMaxDistinctValuesPerDriver();
        this.bloomFilterMaxDistinctValuesPerDriver = dynamicFilterConfig.getBloomFilterMaxDistinctValuesPerDriver();
        this.partitionedBloomFilterMaxDistinctValuesPerDriver = dynamicFilterConfig.getPartitionedBloomFilterMaxDistinctValuesPerDriver();
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.hashCompiler = requireNonNull(hashCompiler, "hashCompiler is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.cacheManagerRegistry = requireNonNull(cacheManagerRegistry, "cacheManagerRegistry is null");
        this.cachePerformanceTracker = requireNonNull(cachePerformanceTracker, "cachePerformanceTracker is null");
        this.tupleDomainCodec = requireNonNull(tupleDomainCodec, "tupleDomainCodec is null");
        this.positionsAppenderFactory = new PositionsAppenderFactory(blockTypeOperators);
        this.version = requireNonNull(version, "version is null");
        this.maxMethodComplexity = compilerConfig.getRowExpressionMaxMethodComplexity();
        this.specializeAggregationLoops = compilerConfig.isSpecializeAggregationLoops();
        this.columnarFilterSubexpressionEvaluationEnabled = compilerConfig.isColumnarFilterSubExpressionEvaluationEnabled();
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            PartitioningScheme partitioningScheme,
            OptionalInt outputSkewedBucketCount,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer)
    {
        List<Symbol> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(taskContext, plan, outputLayout, partitionedSourceOrder, new TaskOutputFactory(outputBuffer));
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        return -1;
                    }
                    return outputLayout.indexOf(argument.getColumn());
                })
                .collect(toImmutableList());
        List<Optional<NullableValue>> partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        return Optional.of(argument.getConstant());
                    }
                    return Optional.<NullableValue>empty();
                })
                .collect(toImmutableList());
        List<Type> partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        return argument.getConstant().getType();
                    }
                    return argument.getColumn().type();
                })
                .collect(toImmutableList());

        PartitionFunction partitionFunction;
        Optional<SkewedPartitionRebalancer> skewedPartitionRebalancer = Optional.empty();
        int taskCount = getTaskCount(partitioningScheme);
        if (outputSkewedBucketCount.isPresent()) {
            partitionFunction = createPartitionFunction(taskContext.getSession(), partitionFunctionProvider, partitioningScheme.getPartitioning().getHandle(), outputSkewedBucketCount.getAsInt(), partitionChannelTypes);
            int partitionedWriterCount = getPartitionedWriterCountBasedOnMemory(taskContext.getSession());
            // Keep the task bucket count to 50% of total local writers
            int taskBucketCount = (int) ceil(0.5 * partitionedWriterCount);
            skewedPartitionRebalancer = Optional.of(new SkewedPartitionRebalancer(
                    partitionFunction.partitionCount(),
                    taskCount,
                    taskBucketCount,
                    getWriterScalingMinDataProcessed(taskContext.getSession()).toBytes(),
                    getSkewedPartitionMinDataProcessedRebalanceThreshold(taskContext.getSession()).toBytes()));
        }
        else {
            partitionFunction = partitionFunctionProvider.getPartitionFunction(
                    taskContext.getSession(),
                    partitioningScheme.getPartitioning().getHandle(),
                    partitionChannelTypes,
                    partitioningScheme.getBucketToPartition()
                            .orElseThrow(() -> new IllegalArgumentException("Bucket to partition must be set before a partition function can be created")));
        }
        OptionalInt nullChannel = OptionalInt.empty();
        Set<Symbol> partitioningColumns = partitioningScheme.getPartitioning().getColumns();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        Optional<Slice> exchangeEncryptionKey = ExchangeEncryptionKey.keyFor(taskContext.getSession(), outputBuffer);

        return plan(
                taskContext,
                plan,
                outputLayout,
                partitionedSourceOrder,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize,
                        positionsAppenderFactory,
                        exchangeEncryptionKey,
                        taskContext.newAggregateMemoryContext(),
                        getPagePartitioningBufferPoolSize(taskContext.getSession()),
                        skewedPartitionRebalancer));
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            List<Symbol> outputLayout,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, metadata, alternativeChooser);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session), context);
        Function<Page, Page> pagePreprocessor = isSpooledOutput(session, physicalOperation) ? LocalExecutionPlanner::validateSpooledLayoutProcessor : enforceLoadedLayoutProcessor(outputLayout, physicalOperation.getLayout());
        List<Type> outputTypes = outputLayout.stream()
                .map(Symbol::type)
                .collect(toImmutableList());

        context.addDriverFactory(
                true,
                new PhysicalOperation(
                        outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                createExchangePagesSerdeFactory(plannerContext.getBlockEncodingSerde(), session)),
                        ImmutableMap.of(),
                        physicalOperation),
                context);

        // notify operator factories that planning has completed
        context.getDriverFactories().forEach(SplitDriverFactory::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder);
    }

    private static boolean isSpooledOutput(Session session, PhysicalOperation operation)
    {
        if (session.getQueryDataEncoding().isEmpty()) {
            return false;
        }
        return operation instanceof SpooledPhysicalOperation;
    }

    private class LocalExecutionPlanContext
    {
        private final TaskContext taskContext;
        private final Metadata metadata;

        private final AlternativeChooser alternativeChooser;
        private final List<SplitDriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        // this is shared with all subContexts
        private final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private Optional<CacheContext> cacheContext = Optional.empty();
        private OptionalInt driverInstanceCount = OptionalInt.empty();
        // Routing constraint that the enclosing planner imposes on HASH local exchanges in this
        // sub-pipeline. Not inherited via createSubContext — each sub-pipeline starts UNCONSTRAINED.
        private HashExchangeConstraint hashExchangeConstraint = HashExchangeConstraint.UNCONSTRAINED;

        public LocalExecutionPlanContext(TaskContext taskContext, Metadata metadata, AlternativeChooser alternativeChooser)
        {
            this(taskContext,
                    metadata,
                    alternativeChooser,
                    new ArrayList<>(),
                    Optional.empty(),
                    new AtomicInteger(0));
        }

        private LocalExecutionPlanContext(
                TaskContext taskContext,
                Metadata metadata,
                AlternativeChooser alternativeChooser,
                List<SplitDriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                AtomicInteger nextPipelineId)
        {
            this.taskContext = taskContext;
            this.metadata = metadata;
            this.alternativeChooser = alternativeChooser;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.nextPipelineId = nextPipelineId;
        }

        public void addDriverFactory(boolean outputDriver, PhysicalOperation physicalOperation, LocalExecutionPlanContext context)
        {
            boolean inputDriver = context.isInputDriver();
            OptionalInt driverInstances = context.getDriverInstanceCount();
            List<OperatorFactory> operatorFactories = physicalOperation.pipelineTail;
            addLookupOuterDrivers(outputDriver, operatorFactories);
            if (physicalOperation.pipelineHeadAlternatives.isEmpty()) {
                addDriverFactory(inputDriver, outputDriver, operatorFactories, driverInstances);
            }
            else {
                // we have alternatives, we need to extend them to the end of the pipeline and create AlternativesAwareDriverFactory
                List<OperatorFactory> commonOperators = physicalOperation.pipelineTail.stream()
                        .map(SharedOperatorFactory::new)
                        .collect(toImmutableList());
                int pipelineId = getNextPipelineId();
                Map<TableHandle, DriverFactory> alternatives = Maps.transformValues(physicalOperation.pipelineHeadAlternatives,
                        alternative -> new DriverFactory(
                                pipelineId,
                                inputDriver,
                                outputDriver,
                                ImmutableList.<OperatorFactory>builder()
                                        .addAll(alternative)
                                        .addAll(commonOperators)
                                        .build(),
                                driverInstances));
                Optional<CacheDriverFactory> cacheDriverFactory = context.getCacheContext()
                        .map(cacheContext -> new CacheDriverFactory(
                                taskContext.getSession(),
                                pageSourceManager,
                                cacheManagerRegistry,
                                tupleDomainCodec,
                                cacheContext.getOriginalTableHandle(),
                                cacheContext.getPlanSignature(),
                                cacheContext.getCommonColumnHandles(),
                                cacheContext.getDynamicFilterSupplier(),
                                ImmutableList.copyOf(alternatives.values()),
                                cacheStats,
                                cachePerformanceTracker));
                driverFactories.add(new AlternativesAwareDriverFactory(
                        alternativeChooser,
                        taskContext.getSession(),
                        alternatives,
                        physicalOperation.chooseAlternativePlanNodeId.get(),
                        cacheDriverFactory,
                        pipelineId,
                        inputDriver,
                        outputDriver,
                        driverInstances));
            }
        }

        private void addLookupOuterDrivers(boolean isOutputDriver, List<OperatorFactory> operatorFactories)
        {
            // For an outer join on the lookup side (RIGHT or FULL) add an additional
            // driver to output the unused rows in the lookup source
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory lookupJoin)) {
                    continue;
                }

                Optional<JoinOperatorFactory.OuterOperatorFactory> outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    int expectedOuterOperatorCount = outerOperatorFactoryResult.get().getPartitionCount().orElse(1);

                    addDriverFactory(false, isOutputDriver, newOperators.build(), OptionalInt.of(expectedOuterOperatorCount));
                }
            }
        }

        private void addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances)
        {
            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances));
        }

        private List<SplitDriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public StageId getStageId()
        {
            return taskContext.getTaskId().stageId();
        }

        public TaskId getTaskId()
        {
            return taskContext.getTaskId();
        }

        public LocalDynamicFiltersCollector getDynamicFiltersCollector()
        {
            return taskContext.getLocalDynamicFiltersCollector();
        }

        private void registerCoordinatorDynamicFilters(List<DynamicFilters.Descriptor> dynamicFilters)
        {
            Set<DynamicFilterId> consumedFilterIds = dynamicFilters.stream()
                    .map(DynamicFilters.Descriptor::getId)
                    .collect(toImmutableSet());
            getDynamicFiltersCollector().register(consumedFilterIds);
        }

        private TaskContext getTaskContext()
        {
            return taskContext;
        }

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
        }

        private int getNextPipelineId()
        {
            return nextPipelineId.getAndIncrement();
        }

        private int getNextOperatorId()
        {
            return nextOperatorId++;
        }

        private boolean isInputDriver()
        {
            return inputDriver;
        }

        private void setInputDriver(boolean inputDriver)
        {
            this.inputDriver = inputDriver;
        }

        public LocalExecutionPlanContext createSubContext()
        {
            checkState(indexSourceContext.isEmpty(), "index build plan cannot have sub-contexts");
            return new LocalExecutionPlanContext(taskContext, metadata, alternativeChooser, driverFactories, indexSourceContext, nextPipelineId);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, metadata, alternativeChooser, driverFactories, Optional.of(indexSourceContext), nextPipelineId);
        }

        public Optional<CacheContext> getCacheContext()
        {
            return cacheContext;
        }

        public void setCacheContext(CacheContext cacheContext)
        {
            checkState(this.cacheContext.isEmpty(), "cacheContext is already set");
            this.cacheContext = Optional.of(requireNonNull(cacheContext, "cacheContext is null"));
        }

        public OptionalInt getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            if (this.driverInstanceCount.isPresent()) {
                checkState(this.driverInstanceCount.getAsInt() == driverInstanceCount, "driverInstance count already set to %s", this.driverInstanceCount.getAsInt());
            }
            this.driverInstanceCount = OptionalInt.of(driverInstanceCount);
        }

        public void setHashExchangeConstraint(HashExchangeConstraint constraint)
        {
            this.hashExchangeConstraint = requireNonNull(constraint, "constraint is null");
        }

        public HashExchangeConstraint getHashExchangeConstraint()
        {
            return hashExchangeConstraint;
        }
    }

    /**
     * Routing constraint applied to a HASH local exchange. Paired HASH pipelines of a partitioned
     * lookup join must agree on hash function: GpuLocalExchange uses cuDF MURMUR3 while the host
     * LE uses Trino's InterpretedHashGenerator, so the probe and build pipelines must both run
     * on the same backend (both GPU or both CPU) or rows get dropped silently. The join planner
     * declares which backend a sub-pipeline belongs to via this constraint; the local-exchange
     * planner falls back to host LE whenever the constraint is {@link #HOST_ONLY}.
     */
    public enum HashExchangeConstraint
    {
        UNCONSTRAINED,
        HOST_ONLY,
    }

    private static class CacheContext
    {
        private final TableHandle originalTableHandle;
        private final PlanSignatureWithPredicate planSignature;
        private final Map<CacheColumnId, ColumnHandle> commonColumnHandles;
        private final Supplier<StaticDynamicFilter> dynamicFilterSupplier;

        public CacheContext(
                TableHandle originalTableHandle,
                LoadCachedDataPlanNode loadCacheData,
                Supplier<StaticDynamicFilter> dynamicFilterSupplier)
        {
            requireNonNull(loadCacheData, "loadCacheData is null");
            this.originalTableHandle = requireNonNull(originalTableHandle, "originalTableHandle is null");
            this.planSignature = loadCacheData.getPlanSignature();
            this.commonColumnHandles = loadCacheData.getCommonColumnHandles();
            this.dynamicFilterSupplier = requireNonNull(dynamicFilterSupplier, "dynamicFilterSupplier is null");
        }

        public TableHandle getOriginalTableHandle()
        {
            return originalTableHandle;
        }

        public PlanSignatureWithPredicate getPlanSignature()
        {
            return planSignature;
        }

        public Map<CacheColumnId, ColumnHandle> getCommonColumnHandles()
        {
            return commonColumnHandles;
        }

        public Supplier<StaticDynamicFilter> getDynamicFilterSupplier()
        {
            return dynamicFilterSupplier;
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<Symbol, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<SplitDriverFactory> driverFactories;
        private final List<PlanNodeId> partitionedSourceOrder;

        public LocalExecutionPlan(List<SplitDriverFactory> driverFactories, List<PlanNodeId> partitionedSourceOrder)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.partitionedSourceOrder = ImmutableList.copyOf(requireNonNull(partitionedSourceOrder, "partitionedSourceOrder is null"));
        }

        public List<SplitDriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getPartitionedSourceOrder()
        {
            return partitionedSourceOrder;
        }
    }

    private class Visitor
            extends PlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        private final Session session;
        private final IrExpressionEvaluator evaluator;

        private Visitor(Session session)
        {
            this.session = session;
            evaluator = plannerContext.getExpressionEvaluator();
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkArgument(node.getRetryPolicy() == NONE, "unexpected retry policy: %s", node.getRetryPolicy());

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            Map<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.orderingList();

            List<Type> types = getSourceOperatorTypes(node);
            List<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    directExchangeClientSupplier,
                    createExchangePagesSerdeFactory(plannerContext.getBlockEncodingSerde(), session),
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (context.getDriverInstanceCount().isEmpty()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            List<Type> outputTypes = mappedCopy(node.getOutputSymbols(), Symbol::type);
            OperatorFactory operatorFactory = new ExchangeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    directExchangeClientSupplier,
                    createExchangePagesSerdeFactory(plannerContext.getBlockEncodingSerde(), session),
                    node.getRetryPolicy(),
                    exchangeManagerRegistry,
                    outputTypes);

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            ExplainAnalyzeContext analyzeContext = explainAnalyzeContext
                    .orElseThrow(() -> new IllegalStateException("ExplainAnalyze can only run on coordinator"));
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    analyzeContext.getQueryPerformanceFetcher(),
                    metadata,
                    plannerContext.getFunctionManager(),
                    node.isVerbose(),
                    version);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            Session session = context.taskContext.getSession();
            PhysicalOperation operation = node.getSource().accept(this, context);

            if (session.getQueryDataEncoding().isEmpty()) {
                return operation;
            }

            QueryDataEncoder.Factory encoderFactory = session
                    .getQueryDataEncoding()
                    .map(encoders::get)
                    .orElseThrow(() -> new IllegalStateException("Spooled query encoding was not found"));

            List<String> columnNames = node.getColumnNames();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            ImmutableList.Builder<OutputColumn> outputColumnBuilder = ImmutableList.builderWithExpectedSize(node.getColumnNames().size());
            for (int i = 0; i < columnNames.size(); i++) {
                outputColumnBuilder.add(new OutputColumn(operation.layout.get(outputSymbols.get(i)), columnNames.get(i), outputSymbols.get(i).type()));
            }
            List<OutputColumn> encodingLayout = outputColumnBuilder.build();
            List<OutputColumn> unsupported = encoderFactory.unsupported(session, encodingLayout);
            if (!unsupported.isEmpty()) {
                throw new TrinoException(SERIALIZATION_ERROR, "Output columns %s are not supported for spooling encoding '%s'".formatted(unsupported, encoderFactory.encoding()));
            }

            OutputSpoolingOperatorFactory outputSpoolingOperatorFactory = new OutputSpoolingOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    () -> encoderFactory.create(session, encodingLayout),
                    spoolingManager.orElseThrow());

            return new SpooledPhysicalOperation(outputSpoolingOperatorFactory, operation);
        }

        @Override
        public PhysicalOperation visitRowNumber(RowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());

            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    10_000,
                    hashStrategyCompiler);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        @Override
        public PhysicalOperation visitTopNRanking(TopNRankingNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());
            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<Type> sortTypes = sortChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderingScheme().ordering(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean isPartial = node.isPartial();
            Optional<DataSize> maxPartialTopNMemorySize = isPartial ? Optional.of(SystemSessionProperties.getMaxPartialTopNMemory(session)).filter(
                    maxSize -> maxSize.compareTo(DataSize.ofBytes(0)) > 0) : Optional.empty();
            OperatorFactory operatorFactory = new TopNRankingOperator.TopNRankingOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getRankingType(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    node.getMaxRankingPerPartition(),
                    isPartial,
                    1000,
                    maxPartialTopNMemorySize,
                    hashStrategyCompiler,
                    orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrder),
                    blockTypeOperators);

            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrder = orderingScheme.orderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> sortKeyChannelForStartComparison = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();
                Optional<Integer> sortKeyChannelForEndComparison = Optional.empty();
                Optional<Integer> sortKeyChannel = Optional.empty();
                Optional<FrameInfo.Ordering> ordering = Optional.empty();

                Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getSortKeyCoercedForFrameStartComparison().isPresent()) {
                    sortKeyChannelForStartComparison = Optional.of(source.getLayout().get(frame.getSortKeyCoercedForFrameStartComparison().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }
                if (frame.getSortKeyCoercedForFrameEndComparison().isPresent()) {
                    sortKeyChannelForEndComparison = Optional.of(source.getLayout().get(frame.getSortKeyCoercedForFrameEndComparison().get()));
                }
                if (node.getOrderingScheme().isPresent()) {
                    // the following fields are only used for frame type RANGE
                    // in such case, there is a single sort channel
                    sortKeyChannel = Optional.of(sortChannels.get(0));
                    ordering = Optional.of(sortOrder.get(0).isAscending() ? ASCENDING : DESCENDING);
                }
                FrameInfo frameInfo = new FrameInfo(
                        frame.getType(),
                        frame.getStartType(),
                        frameStartChannel,
                        sortKeyChannelForStartComparison,
                        frame.getEndType(),
                        frameEndChannel,
                        sortKeyChannelForEndComparison,
                        sortKeyChannel,
                        ordering);

                WindowNode.Function function = entry.getValue();
                ResolvedFunction resolvedFunction = function.getResolvedFunction();
                ArrayList<Integer> argumentChannels = new ArrayList<>();
                for (Expression argument : function.getArguments()) {
                    if (!(argument instanceof Lambda)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        argumentChannels.add(source.getLayout().get(argumentSymbol));
                    }
                }
                Symbol symbol = entry.getKey();

                Type type = resolvedFunction.signature().getReturnType();

                List<Lambda> lambdas = function.getArguments().stream()
                        .filter(Lambda.class::isInstance)
                        .map(Lambda.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                WindowFunctionSupplier windowFunctionSupplier;
                if (resolvedFunction.functionKind() == FunctionKind.AGGREGATE) {
                    AggregationWindowFunctionSupplier targetFunction = getAggregationWindowFunctionSupplier(resolvedFunction);
                    List<Class<?>> lambdaInterfaces = targetFunction.getLambdaInterfaces();
                    Function<List<Supplier<Object>>, WindowAccumulator> accumulatorSupplier = targetFunction::createWindowAccumulator;

                    if (function.getOrderingScheme().isPresent()) {
                        OrderingScheme orderingScheme = function.getOrderingScheme().orElseThrow();
                        List<Symbol> sortKeys = orderingScheme.orderBy();
                        List<SortOrder> sortOrders = sortKeys.stream()
                                .map(orderingScheme::ordering)
                                .collect(toImmutableList());
                        ImmutableList.Builder<Integer> sortKeysArgumentsBuilder = ImmutableList.builder();
                        sortKeys.forEach(orderingArgumentSymbol -> {
                            argumentChannels.add(source.getLayout().get(orderingArgumentSymbol));
                            sortKeysArgumentsBuilder.add(argumentChannels.size() - 1); // last added argument
                        });

                        List<Type> argumentTypes = argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList());

                        List<Integer> sortKeysArguments = sortKeysArgumentsBuilder.build();
                        Function<List<Supplier<Object>>, WindowAccumulator> finalAccumulatorSupplier = accumulatorSupplier;
                        accumulatorSupplier = lambdaProviders ->
                                new OrderedWindowAccumulator(
                                        pagesIndexFactory,
                                        finalAccumulatorSupplier.apply(lambdaProviders),
                                        argumentTypes,
                                        sortKeysArguments,
                                        sortOrders);
                    }

                    if (function.isDistinct()) {
                        List<Type> argumentTypes = argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList());

                        Function<List<Supplier<Object>>, WindowAccumulator> finalAccumulatorSupplier = accumulatorSupplier;
                        List<Integer> argumentChannelsFinal = ImmutableList.copyOf(argumentChannels);
                        accumulatorSupplier = lambdaProviders -> new DistinctWindowAccumulator(
                                finalAccumulatorSupplier.apply(lambdaProviders),
                                argumentTypes,
                                argumentChannelsFinal,
                                hashStrategyCompiler,
                                session,
                                pagesIndexFactory);
                    }

                    windowFunctionSupplier = windowAggregationFunctionSupplier(resolvedFunction, lambdaInterfaces, accumulatorSupplier);
                }
                else {
                    windowFunctionSupplier = plannerContext.getFunctionManager().getWindowFunctionSupplier(resolvedFunction);
                }

                List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
                WindowFunctionDefinition windowFunction = window(windowFunctionSupplier, type, frameInfo, function.isIgnoreNulls(), lambdaProviders, ImmutableList.copyOf(argumentChannels));

                windowFunctionsBuilder.add(windowFunction);
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session),
                    spillerFactory,
                    orderingCompiler,
                    ImmutableList.of(),
                    new RegularPartitionerSupplier());

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private WindowFunctionSupplier windowAggregationFunctionSupplier(ResolvedFunction resolvedFunction, List<Class<?>> lambdaInterfaces, Function<List<Supplier<Object>>, WindowAccumulator> accumulatorSupplier)
        {
            return new WindowFunctionSupplier()
            {
                @Override
                public WindowFunction createWindowFunction(boolean ignoreNulls, List<Supplier<Object>> lambdaProviders)
                {
                    AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(resolvedFunction);
                    boolean hasRemoveInput = aggregationImplementation.getWindowAccumulator().isPresent();
                    return new AggregateWindowFunction(
                            () -> accumulatorSupplier.apply(lambdaProviders),
                            hasRemoveInput);
                }

                @Override
                public List<Class<?>> getLambdaInterfaces()
                {
                    return lambdaInterfaces;
                }
            };
        }

        @Override
        public PhysicalOperation visitPatternRecognition(PatternRecognitionNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrder = orderingScheme.orderingList();
            }

            // The output order for pattern recognition operation is defined as follows:
            // - for ONE ROW PER MATCH: partition by symbols, then measures,
            // - for ALL ROWS PER MATCH: partition by symbols, order by symbols, measures, remaining input symbols,
            // - for WINDOW: all input symbols, then window functions (including measures).
            // The operator produces output in the following order:
            // - for ONE ROW PER MATCH: partition by symbols, then measures,
            // - otherwise all input symbols, then window functions and measures.
            // There is no need to shuffle channels for output. Any upstream operator will pick them in preferred order using output mappings.

            // input channels to be passed directly to output
            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();

            // all output symbols mapped to output channels
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();

            int nextOutputChannel;

            if (node.getRowsPerMatch() == ONE) {
                outputChannels.addAll(partitionChannels);
                nextOutputChannel = partitionBySymbols.size();
                for (int i = 0; i < partitionBySymbols.size(); i++) {
                    outputMappings.put(partitionBySymbols.get(i), i);
                }
            }
            else {
                outputChannels.addAll(IntStream.range(0, source.getTypes().size())
                        .boxed()
                        .collect(toImmutableList()));
                nextOutputChannel = source.getTypes().size();
                outputMappings.putAll(source.getLayout());
            }

            // measures go in remaining channels starting after the last channel from the source operator, one per channel
            for (Entry<Symbol, Measure> measure : node.getMeasures().entrySet()) {
                outputMappings.put(measure.getKey(), nextOutputChannel);
                nextOutputChannel++;
            }

            // process window functions
            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            for (Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                // window functions outputs go in remaining channels starting after the last measure channel
                outputMappings.put(entry.getKey(), nextOutputChannel);
                nextOutputChannel++;

                WindowNode.Function function = entry.getValue();
                ResolvedFunction resolvedFunction = function.getResolvedFunction();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (Expression argument : function.getArguments()) {
                    if (!(argument instanceof Lambda)) {
                        Symbol argumentSymbol = Symbol.from(argument);
                        arguments.add(source.getLayout().get(argumentSymbol));
                    }
                }
                WindowFunctionSupplier windowFunctionSupplier = getWindowFunctionImplementation(resolvedFunction);
                Type type = resolvedFunction.signature().getReturnType();

                List<Lambda> lambdas = function.getArguments().stream()
                        .filter(Lambda.class::isInstance)
                        .map(Lambda.class::cast)
                        .collect(toImmutableList());
                List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                        .filter(FunctionType.class::isInstance)
                        .map(FunctionType.class::cast)
                        .collect(toImmutableList());

                List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, windowFunctionSupplier.getLambdaInterfaces(), functionTypes);
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, function.isIgnoreNulls(), lambdaProviders, arguments.build()));
            }

            // prepare structures specific to PatternRecognitionNode
            // 1. establish a two-way mapping of IrLabels to `int`
            List<IrLabel> primaryLabels = ImmutableList.copyOf(node.getVariableDefinitions().keySet());
            ImmutableList.Builder<String> labelNamesBuilder = ImmutableList.builder();
            ImmutableMap.Builder<IrLabel, Integer> mappingBuilder = ImmutableMap.builder();
            for (int i = 0; i < primaryLabels.size(); i++) {
                IrLabel label = primaryLabels.get(i);
                labelNamesBuilder.add(label.getName());
                mappingBuilder.put(label, i);
            }
            Map<IrLabel, Integer> mapping = mappingBuilder.buildOrThrow();
            List<String> labelNames = labelNamesBuilder.build();

            // 2. rewrite pattern to program
            Program program = IrRowPatternToProgramRewriter.rewrite(node.getPattern(), mapping);

            // 3. prepare common base frame for pattern matching in window
            Optional<FrameInfo> frame = node.getCommonBaseFrame()
                    .map(baseFrame -> {
                        checkArgument(
                                baseFrame.getType() == ROWS &&
                                        baseFrame.getStartType() == CURRENT_ROW,
                                "invalid base frame");
                        return new FrameInfo(
                                baseFrame.getType(),
                                baseFrame.getStartType(),
                                Optional.empty(),
                                Optional.empty(),
                                baseFrame.getEndType(),
                                baseFrame.getEndValue().map(source.getLayout()::get),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty());
                    });

            ConnectorSession connectorSession = session.toConnectorSession();
            // 4. prepare label evaluations (LabelEvaluator is to be instantiated once per Partition)

            // during pattern matching, each thread will have a list of aggregations necessary for label evaluations.
            // the list of aggregations for a thread will be produced at thread creation time from this supplier list, respecting the order.
            // pointers in LabelEvaluator and ThreadEquivalence will access aggregations by position in list.
            int matchAggregationIndex = 0;
            ImmutableList.Builder<MatchAggregationInstantiator> labelEvaluationsAggregations = ImmutableList.builder();
            // runtime-evaluated aggregation arguments will appear in additional channels after all source channels
            int firstUnusedChannel = source.getLayout().values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            ImmutableList.Builder<ArgumentComputationSupplier> labelEvaluationsAggregationArguments = ImmutableList.builder();
            ImmutableList.Builder<List<PhysicalValueAccessor>> evaluationsValuePointers = ImmutableList.builder();

            ImmutableList.Builder<MatchAggregationLabelDependency> aggregationsLabelDependencies = ImmutableList.builder();

            ImmutableList.Builder<EvaluationSupplier> evaluationsBuilder = ImmutableList.builder();
            for (ExpressionAndValuePointers expressionAndValuePointers : node.getVariableDefinitions().values()) {
                // compile the rewritten expression
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, firstUnusedChannel, matchAggregationIndex);

                firstUnusedChannel = valueAccessors.getFirstUnusedChannel();
                matchAggregationIndex = valueAccessors.getAggregationIndex();

                // record aggregations
                labelEvaluationsAggregations.addAll(valueAccessors.getAggregations());

                // record aggregation argument computations
                labelEvaluationsAggregationArguments.addAll(valueAccessors.getAggregationArguments());

                // record aggregation label dependencies and value accessors for ThreadEquivalence
                aggregationsLabelDependencies.addAll(valueAccessors.getLabelDependencies());
                evaluationsValuePointers.add(valueAccessors.getValueAccessors());

                // build label evaluation
                evaluationsBuilder.add(new EvaluationSupplier(pageProjectionSupplier, valueAccessors.getValueAccessors(), labelNames, connectorSession));
            }
            List<EvaluationSupplier> labelEvaluations = evaluationsBuilder.build();

            // 5. prepare measures computations

            matchAggregationIndex = 0;
            ImmutableList.Builder<MatchAggregationInstantiator> measureComputationsAggregations = ImmutableList.builder();
            // runtime-evaluated aggregation arguments will appear in additional channels after all source channels
            // measure computations will use a different instance of WindowIndex than the label evaluations
            firstUnusedChannel = source.getLayout().values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            ImmutableList.Builder<ArgumentComputationSupplier> measureComputationsAggregationArguments = ImmutableList.builder();

            ImmutableList.Builder<MeasureComputationSupplier> measuresBuilder = ImmutableList.builder();
            for (Measure measure : node.getMeasures().values()) {
                ExpressionAndValuePointers expressionAndValuePointers = measure.getExpressionAndValuePointers();

                // compile the rewritten expression
                Supplier<PageProjection> pageProjectionSupplier = prepareProjection(expressionAndValuePointers);

                // prepare physical value accessors to provide input for the expression
                ValueAccessors valueAccessors = preparePhysicalValuePointers(expressionAndValuePointers, mapping, source, connectorSession, firstUnusedChannel, matchAggregationIndex);

                firstUnusedChannel = valueAccessors.getFirstUnusedChannel();
                matchAggregationIndex = valueAccessors.getAggregationIndex();

                // record aggregations
                measureComputationsAggregations.addAll(valueAccessors.getAggregations());

                // record aggregation argument computations
                measureComputationsAggregationArguments.addAll(valueAccessors.getAggregationArguments());

                // build measure computation
                measuresBuilder.add(new MeasureComputationSupplier(pageProjectionSupplier, valueAccessors.getValueAccessors(), labelNames, connectorSession));
            }
            List<MeasureComputationSupplier> measureComputations = measuresBuilder.build();

            // 6. prepare SKIP TO navigation
            Optional<LogicalIndexNavigation> skipToNavigation = Optional.empty();
            if (!node.getSkipToLabels().isEmpty()) {
                boolean last = node.getSkipToPosition().equals(LAST);
                skipToNavigation = Optional.of(new LogicalIndexPointer(node.getSkipToLabels(), last, false, 0, 0).toLogicalIndexNavigation(mapping));
            }

            // 7. pass additional info like: rowsPerMatch, skipToPosition, initial to the WindowPartition factory supplier
            PartitionerSupplier partitionerSupplier = new PatternRecognitionPartitionerSupplier(
                    measureComputations,
                    measureComputationsAggregations.build(),
                    measureComputationsAggregationArguments.build(),
                    frame,
                    node.getRowsPerMatch(),
                    skipToNavigation,
                    node.getSkipToPosition(),
                    node.isInitial(),
                    new Matcher(program, evaluationsValuePointers.build(), aggregationsLabelDependencies.build(), labelEvaluationsAggregations.build()),
                    labelEvaluations,
                    labelEvaluationsAggregationArguments.build(),
                    labelNames);

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory,
                    isSpillEnabled(session),
                    spillerFactory,
                    orderingCompiler,
                    node.getMeasures().values().stream()
                            .map(Measure::getType)
                            .collect(toImmutableList()),
                    partitionerSupplier);

            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private WindowFunctionSupplier getWindowFunctionImplementation(ResolvedFunction resolvedFunction)
        {
            if (resolvedFunction.functionKind() == FunctionKind.AGGREGATE) {
                return getAggregationWindowFunctionSupplier(resolvedFunction);
            }
            return plannerContext.getFunctionManager().getWindowFunctionSupplier(resolvedFunction);
        }

        private AggregationWindowFunctionSupplier getAggregationWindowFunctionSupplier(ResolvedFunction resolvedFunction)
        {
            checkArgument(
                    resolvedFunction.functionKind() == FunctionKind.AGGREGATE,
                    "Expected %s to be AGGREGATE function, but got %s",
                    resolvedFunction.functionId(),
                    resolvedFunction.functionKind());
            return uncheckedCacheGet(aggregationWindowFunctionSupplierCache, new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()), () -> {
                AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(resolvedFunction);
                return new AggregationWindowFunctionSupplier(
                        resolvedFunction.signature(),
                        aggregationImplementation,
                        resolvedFunction.functionNullability());
            });
        }

        private Supplier<PageProjection> prepareProjection(ExpressionAndValuePointers expressionAndValuePointers)
        {
            Expression rewritten = expressionAndValuePointers.getExpression();

            // prepare input layout for compilation
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();

            List<ExpressionAndValuePointers.Assignment> assignments = expressionAndValuePointers.getAssignments();
            for (int i = 0; i < assignments.size(); i++) {
                ExpressionAndValuePointers.Assignment assignment = assignments.get(i);
                inputLayout.put(assignment.symbol(), i);
            }

            // compile expression using input layout and input types
            return pageFunctionCompiler.compileProjection(rewritten, inputLayout.buildOrThrow(), Optional.empty());
        }

        private ValueAccessors preparePhysicalValuePointers(
                ExpressionAndValuePointers expressionAndValuePointers,
                Map<IrLabel, Integer> mapping,
                PhysicalOperation source,
                ConnectorSession connectorSession,
                int firstUnusedChannel,
                int matchAggregationIndex)
        {
            Map<Symbol, Integer> sourceLayout = source.getLayout();

            ImmutableList.Builder<MatchAggregationInstantiator> matchAggregations = ImmutableList.builder();

            // runtime-evaluated aggregation arguments mapped to free channel slots
            ImmutableList.Builder<ArgumentComputationSupplier> aggregationArguments = ImmutableList.builder();

            // for thread equivalence
            ImmutableList.Builder<MatchAggregationLabelDependency> labelDependencies = ImmutableList.builder();

            ImmutableList.Builder<PhysicalValueAccessor> valueAccessors = ImmutableList.builder();
            for (ExpressionAndValuePointers.Assignment assignment : expressionAndValuePointers.getAssignments()) {
                switch (assignment.valuePointer()) {
                    case ClassifierValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(
                                CLASSIFIER,
                                VARCHAR,
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    case MatchNumberValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(MATCH_NUMBER, BIGINT, LogicalIndexNavigation.NO_OP));
                    }
                    case ScalarValuePointer pointer -> {
                        valueAccessors.add(new PhysicalValuePointer(
                                getOnlyElement(getChannelsForSymbols(ImmutableList.of(pointer.getInputSymbol()), sourceLayout)),
                                pointer.getInputSymbol().type(),
                                pointer.getLogicalIndexPointer().toLogicalIndexNavigation(mapping)));
                    }
                    case AggregationValuePointer pointer -> {
                        boolean classifierInvolved = false;

                        ResolvedFunction resolvedFunction = pointer.getFunction();
                        AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(pointer.getFunction());

                        ImmutableList.Builder<Entry<Expression, Type>> builder = ImmutableList.builder();
                        List<Type> signatureTypes = resolvedFunction.signature().getArgumentTypes();
                        for (int i = 0; i < pointer.getArguments().size(); i++) {
                            builder.add(new SimpleEntry<>(pointer.getArguments().get(i), signatureTypes.get(i)));
                        }
                        Map<Boolean, List<Entry<Expression, Type>>> arguments = builder.build().stream()
                                .collect(partitioningBy(entry -> entry.getKey() instanceof Lambda));

                        // handle lambda arguments
                        List<Lambda> lambdas = arguments.get(true).stream()
                                .map(Entry::getKey)
                                .map(Lambda.class::cast)
                                .collect(toImmutableList());

                        List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                                .filter(FunctionType.class::isInstance)
                                .map(FunctionType.class::cast)
                                .collect(toImmutableList());

                        // TODO when we support lambda arguments: lambda cannot have runtime-evaluated symbols -- add check in the Analyzer
                        List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, aggregationImplementation.getLambdaInterfaces(), functionTypes);

                        // handle non-lambda arguments
                        List<Integer> valueChannels = new ArrayList<>();

                        Optional<Symbol> classifierArgumentSymbol = pointer.getClassifierSymbol();
                        Optional<Symbol> matchNumberArgumentSymbol = pointer.getMatchNumberSymbol();
                        Set<Symbol> runtimeEvaluatedSymbols = ImmutableSet.of(classifierArgumentSymbol, matchNumberArgumentSymbol).stream()
                                .flatMap(Optional::stream)
                                .collect(toImmutableSet());

                        for (Entry<Expression, Type> argumentWithType : arguments.get(false)) {
                            Expression argument = argumentWithType.getKey();
                            boolean isRuntimeEvaluated = !(argument instanceof Reference) || runtimeEvaluatedSymbols.contains(Symbol.from(argument));
                            if (isRuntimeEvaluated) {
                                List<Symbol> argumentInputSymbols = ImmutableList.copyOf(SymbolsExtractor.extractUnique(argument));
                                Supplier<PageProjection> argumentProjectionSupplier = prepareArgumentProjection(argument, argumentInputSymbols);

                                List<Integer> argumentInputChannels = new ArrayList<>();
                                for (Symbol symbol : argumentInputSymbols) {
                                    if (classifierArgumentSymbol.isPresent() && symbol.equals(classifierArgumentSymbol.get())) {
                                        classifierInvolved = true;
                                        argumentInputChannels.add(CLASSIFIER);
                                    }
                                    else if (matchNumberArgumentSymbol.isPresent() && symbol.equals(matchNumberArgumentSymbol.get())) {
                                        argumentInputChannels.add(MATCH_NUMBER);
                                    }
                                    else {
                                        argumentInputChannels.add(sourceLayout.get(symbol));
                                    }
                                }

                                Type argumentType = argumentWithType.getValue();
                                ArgumentComputationSupplier argumentComputationSupplier = new ArgumentComputationSupplier(argumentProjectionSupplier, argumentType, argumentInputChannels, connectorSession);
                                aggregationArguments.add(argumentComputationSupplier);

                                // the runtime-evaluated argument will appear in an extra channel after all input channels
                                valueChannels.add(firstUnusedChannel);
                                firstUnusedChannel++;
                            }
                            else {
                                valueChannels.add(sourceLayout.get(Symbol.from(argument)));
                            }
                        }

                        AggregationWindowFunctionSupplier aggregationWindowFunctionSupplier = uncheckedCacheGet(
                                aggregationWindowFunctionSupplierCache,
                                new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()),
                                () -> new AggregationWindowFunctionSupplier(
                                        resolvedFunction.signature(),
                                        aggregationImplementation,
                                        resolvedFunction.functionNullability()));
                        matchAggregations.add(new MatchAggregationInstantiator(
                                resolvedFunction.signature(),
                                aggregationWindowFunctionSupplier,
                                valueChannels,
                                lambdaProviders,
                                new SetEvaluatorSupplier(pointer.getSetDescriptor(), mapping)));
                        labelDependencies.add(new MatchAggregationLabelDependency(
                                pointer.getSetDescriptor().getLabels().stream()
                                        .map(mapping::get)
                                        .collect(toImmutableSet()),
                                classifierInvolved));
                        valueAccessors.add(new MatchAggregationPointer(matchAggregationIndex));
                        matchAggregationIndex++;
                    }
                }
            }

            return new ValueAccessors(valueAccessors.build(), matchAggregations.build(), matchAggregationIndex, aggregationArguments.build(), firstUnusedChannel, labelDependencies.build());
        }

        private Supplier<PageProjection> prepareArgumentProjection(Expression argument, List<Symbol> inputSymbols)
        {
            // prepare input layout and type provider for compilation
            ImmutableMap.Builder<Symbol, Integer> inputLayout = ImmutableMap.builder();
            for (int i = 0; i < inputSymbols.size(); i++) {
                inputLayout.put(inputSymbols.get(i), i);
            }

            // compile expression using input layout and input types
            return pageFunctionCompiler.compileProjection(argument, inputLayout.buildOrThrow(), Optional.empty());
        }

        @Override
        public PhysicalOperation visitTableFunction(TableFunctionNode node, LocalExecutionPlanContext context)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public PhysicalOperation visitTableFunctionProcessor(TableFunctionProcessorNode node, LocalExecutionPlanContext context)
        {
            TableFunctionProcessorProvider processorProvider = plannerContext.getFunctionManager().getTableFunctionProcessorProvider(node.getHandle());

            if (node.getSource().isEmpty()) {
                List<Type> outputTypes = mappedCopy(node.getOutputSymbols(), Symbol::type);
                OperatorFactory operatorFactory = new LeafTableFunctionOperatorFactory(
                        context.getNextOperatorId(),
                        node.getId(),
                        node.getHandle().catalogHandle(),
                        processorProvider,
                        node.getHandle().functionHandle(),
                        context.getTaskContext().getTableCredentials(node.getId()),
                        outputTypes);
                return new PhysicalOperation(operatorFactory, makeLayout(node));
            }

            PhysicalOperation source = node.getSource().orElseThrow().accept(this, context);

            int properChannelsCount = node.getProperOutputs().size();

            long passThroughSourcesCount = node.getPassThroughSpecifications().stream()
                    .filter(PassThroughSpecification::declaredAsPassThrough)
                    .count();

            List<List<Integer>> requiredChannels = node.getRequiredSymbols().stream()
                    .map(list -> getChannelsForSymbols(list, source.getLayout()))
                    .collect(toImmutableList());

            Optional<Map<Integer, Integer>> markerChannels = node.getMarkerSymbols()
                    .map(map -> map.entrySet().stream()
                            .collect(toImmutableMap(entry -> source.getLayout().get(entry.getKey()), entry -> source.getLayout().get(entry.getValue()))));

            int channel = properChannelsCount;
            ImmutableList.Builder<PassThroughColumnSpecification> passThroughColumnSpecifications = ImmutableList.builder();
            for (PassThroughSpecification specification : node.getPassThroughSpecifications()) {
                // the table function produces one index channel for each source declared as pass-through. They are laid out after the proper channels.
                int indexChannel = specification.declaredAsPassThrough() ? channel++ : -1;
                for (PassThroughColumn column : specification.columns()) {
                    passThroughColumnSpecifications.add(new PassThroughColumnSpecification(column.isPartitioningColumn(), source.getLayout().get(column.symbol()), indexChannel));
                }
            }

            List<Integer> partitionChannels = node.getSpecification()
                    .map(DataOrganizationSpecification::partitionBy)
                    .map(list -> getChannelsForSymbols(list, source.getLayout()))
                    .orElse(ImmutableList.of());

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrders = ImmutableList.of();
            if (node.getSpecification().flatMap(DataOrganizationSpecification::orderingScheme).isPresent()) {
                OrderingScheme orderingScheme = node.getSpecification().flatMap(DataOrganizationSpecification::orderingScheme).orElseThrow();
                sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), source.getLayout());
                sortOrders = orderingScheme.orderingList();
            }

            OperatorFactory operator = new TableFunctionOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    processorProvider,
                    node.getHandle().catalogHandle(),
                    node.getHandle().functionHandle(),
                    properChannelsCount,
                    toIntExact(passThroughSourcesCount),
                    requiredChannels,
                    markerChannels,
                    passThroughColumnSpecifications.build(),
                    node.isPruneWhenEmpty(),
                    partitionChannels,
                    getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitioned()), source.getLayout()),
                    sortChannels,
                    sortOrders,
                    node.getPreSorted(),
                    source.getTypes(),
                    10_000,
                    pagesIndexFactory);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (int i = 0; i < node.getProperOutputs().size(); i++) {
                outputMappings.put(node.getProperOutputs().get(i), i);
            }
            List<Symbol> passThroughSymbols = node.getPassThroughSpecifications().stream()
                    .map(PassThroughSpecification::columns)
                    .flatMap(Collection::stream)
                    .map(PassThroughColumn::symbol)
                    .collect(toImmutableList());
            int outputChannel = properChannelsCount;
            for (Symbol passThroughSymbol : passThroughSymbols) {
                outputMappings.put(passThroughSymbol, outputChannel++);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<PhysicalOperation> gpuOperation = tryPlanGpuTopN(node, source, context);
            if (gpuOperation.isPresent()) {
                return gpuOperation.get();
            }

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();

            List<Type> sortTypes = new ArrayList<>();
            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                int sortChannel = source.getLayout().get(symbol);
                sortTypes.add(source.getTypes().get(sortChannel));
                sortChannels.add(sortChannel);
                sortOrders.add(node.getOrderingScheme().ordering(symbol));
            }

            OperatorFactory operator = TopNOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrders));

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        private Optional<PhysicalOperation> tryPlanGpuTopN(TopNNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            if (!isGpuExecutionEnabled(session) || !source.getTypes().stream().allMatch(GpuTypeConversion::isConvertible)) {
                return Optional.empty();
            }

            Map<Symbol, Integer> sourceLayout = source.getLayout();
            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();

            int[] sortChannels = new int[orderBySymbols.size()];
            ImmutableList.Builder<SortOrder> sortOrders = ImmutableList.builder();

            for (int i = 0; i < orderBySymbols.size(); i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortChannels[i] = sourceLayout.get(symbol);
                sortOrders.add(node.getOrderingScheme().ordering(symbol));
            }

            GpuTopN.Factory gpuTopN = new GpuTopN.Factory(toIntExact(node.getCount()), sortChannels, sortOrders.build());

            return Optional.of(addGpuOperation(
                    gpuTopN,
                    source.getTypes(),
                    source,
                    source.getLayout(),
                    context,
                    node.getId()));
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderingScheme().orderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderingScheme().ordering(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            boolean spillEnabled = isSpillEnabled(session);

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    10_000,
                    orderByChannels,
                    sortOrder.build(),
                    pagesIndexFactory,
                    spillEnabled,
                    Optional.of(spillerFactory),
                    orderingCompiler);

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            // Limit with ties should be rewritten at this point
            checkState(node.getTiesResolvingScheme().isEmpty(), "Limit with ties not supported");

            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    distinctChannels,
                    node.getLimit(),
                    hashStrategyCompiler);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getDistinctGroupingSetSymbols()) {
                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(source.getLayout().get(node.getGroupingColumns().get(output))));
            }

            Map<Symbol, Integer> argumentMappings = new HashMap<>();
            for (Symbol output : node.getAggregationArguments()) {
                int inputChannel = source.getLayout().get(output);

                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(inputChannel));
                argumentMappings.put(output, inputChannel);
            }

            // for every grouping set, create a mapping of all output to input channels (including arguments)
            ImmutableList.Builder<Map<Integer, Integer>> mappings = ImmutableList.builder();
            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableMap.Builder<Integer, Integer> setMapping = ImmutableMap.builder();

                for (Symbol output : groupingSet) {
                    setMapping.put(newLayout.get(output), source.getLayout().get(node.getGroupingColumns().get(output)));
                }

                for (Symbol output : argumentMappings.keySet()) {
                    setMapping.put(newLayout.get(output), argumentMappings.get(output));
                }

                mappings.add(setMapping.buildOrThrow());
            }

            newLayout.put(node.getGroupIdSymbol(), outputChannel);
            outputTypes.add(BIGINT);

            if (isGpuExecutionEnabled(session)) {
                Optional<List<DType>> outputDTypes = toDTypes(outputTypes.build());
                if (outputDTypes.isPresent()) {
                    return addGpuOperation(
                            new GpuGroupId.Factory(mappings.build(), outputDTypes.get()),
                            outputTypes.build(),
                            source,
                            newLayout,
                            context,
                            node.getId());
                }
            }

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    outputTypes.build(),
                    mappings.build());

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<PhysicalOperation> gpuOperation = tryPlanGpuAggregation(node, source, context);
            if (gpuOperation.isPresent()) {
                return gpuOperation.get();
            }

            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(session);
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(session);

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashStrategyCompiler);
            return new PhysicalOperation(operator, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            List<Symbol> outputSymbols = node.getOutputSymbols();
            return visitScanFilterAndProject(context, node.getId(), node.getSource(), Optional.of(node.getPredicate()), Assignments.identity(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<Expression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode filterNode) {
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<Expression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            TableHandle table = null;
            Optional<ConnectorTableCredentials> tableCredentials = Optional.empty();
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            if (sourceNode instanceof TableScanNode tableScanNode) {
                table = tableScanNode.getTable();
                tableCredentials = context.getTaskContext().getTableCredentials(tableScanNode.getId());
                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));
                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    channel++;
                }
            }
            // TODO: This is a simple hack, it will be replaced when we add ability to push down sampling into connectors.
            // SYSTEM sampling is performed in the coordinator by dropping some random splits so the SamplingNode can be skipped here.
            else if (sourceNode instanceof SampleNode sampleNode) {
                checkArgument(sampleNode.getSampleType() == SampleNode.Type.SYSTEM, "%s sampling is not supported", sampleNode.getSampleType());
                return visitScanFilterAndProject(
                        context,
                        planNodeId,
                        sampleNode.getSource(),
                        filterExpression,
                        assignments,
                        outputSymbols);
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.buildOrThrow();

            Optional<Expression> staticFilters = filterExpression.flatMap(this::getStaticFilter);
            InternalDynamicFilter dynamicFilter = filterExpression
                    .filter(_ -> sourceNode instanceof TableScanNode)
                    .map(expression -> getDynamicFilter((TableScanNode) sourceNode, expression, context))
                    .orElse(InternalDynamicFilter.EMPTY);

            List<Expression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            // First, we try to plan execution on the GPU, if that's not supported, we fall back to the CPU.
            if (isGpuExecutionEnabled(session)) {
                Optional<PhysicalOperation> sourceGpuOperation = Optional.empty();
                List<Type> sourceOutputTypes;

                if (columns != null) {
                    sourceOutputTypes = sourceNode.getOutputSymbols().stream()
                            .map(Symbol::type)
                            .collect(toImmutableList());
                    if (pageSourceManager.supportsConnectorGpuPageSource(table.catalogHandle(), table.connectorHandle()) &&
                            // table scan has types supported on the GPU
                            sourceLayout.keySet().stream().map(Symbol::type).allMatch(GpuTypeConversion::isConvertible)) {
                        // TODO (https://starburstdata.atlassian.net/browse/ENG-9785) Support Dynamic Row-Level Filter in GPU-accelerated Table Scan operator?
                        GpuOperator.SourceFactory gpuOperator = new GpuOperator.SourceFactory(
                                context.getNextOperatorId(),
                                sourceNode.getId(),
                                pageSourceManager.createPageSourceProvider(table.catalogHandle()),
                                session,
                                table,
                                tableCredentials,
                                columns,
                                dynamicFilter,
                                sourceOutputTypes);

                        sourceGpuOperation = Optional.of(new PhysicalOperation(gpuOperator, sourceLayout));
                    }
                }
                else {
                    sourceOutputTypes = source.getTypes();
                    List<OperatorFactory> sourcePipeline = source.getPipelineTail();
                    if (!sourcePipeline.isEmpty() && sourcePipeline.getLast() instanceof GpuOperator.BaseFactory) {
                        sourceGpuOperation = Optional.of(source);
                    }
                }

                // Filters and projections are only added when there is a preceding GPU operation
                if (sourceGpuOperation.isPresent() &&
                        // projections have types supported on the GPU
                        projections.stream().map(Expression::type).allMatch(GpuTypeConversion::isConvertible)) {
                    Optional<CompiledExpression> gpuFilter = staticFilters.flatMap(filter -> gpuExpressionCompiler.compileExpression(filter, sourceLayout));
                    if (staticFilters.isPresent() == gpuFilter.isPresent()) {
                        PhysicalOperation gpuOperation = sourceGpuOperation.get();
                        if (gpuFilter.isPresent()) {
                            gpuOperation = addGpuOperation(
                                    new GpuFilter.Factory(gpuFilter.get()),
                                    sourceOutputTypes,
                                    gpuOperation,
                                    gpuOperation.getLayout(),
                                    context,
                                    planNodeId);
                        }

                        Optional<List<CompiledExpression>> gpuProjections = gpuExpressionCompiler.compileExpressions(projections, sourceLayout);
                        if (gpuProjections.isPresent()) {
                            return addGpuOperation(
                                    new GpuProject.Factory(
                                            gpuProjections.get().stream()
                                                    .map(GpuProject.Projection.Gpu::new)
                                                    .collect(toImmutableList())),
                                    getTypes(projections),
                                    gpuOperation,
                                    outputMappings,
                                    context,
                                    planNodeId);
                        }
                    }
                }
            }

            try {
                boolean columnarFilterEvaluationEnabled = isColumnarFilterEvaluationEnabled(session);
                boolean isDebugOutputEnabled = isDebugOutputEnabled(session);
                boolean filterReorderingEnabled = isAdaptiveFilterReorderingEnabled(session);
                Optional<DynamicPageFilter> dynamicPageFilterFactory = Optional.empty();
                if (dynamicFilter != InternalDynamicFilter.EMPTY && isEnableDynamicRowFiltering(session)) {
                    dynamicPageFilterFactory = Optional.of(new DynamicPageFilter(
                            plannerContext,
                            session,
                            ((TableScanNode) sourceNode).getAssignments(),
                            sourceLayout,
                            getDynamicRowFilterSelectivityThreshold(session),
                            filterReorderingEnabled));
                }
                Function<InternalDynamicFilter, PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(
                        columnarFilterEvaluationEnabled,
                        columnarFilterSubexpressionEvaluationEnabled,
                        isDebugOutputEnabled,
                        filterReorderingEnabled,
                        staticFilters,
                        dynamicPageFilterFactory,
                        projections,
                        sourceLayout,
                        Optional.of(context.getStageId() + "_" + planNodeId),
                        OptionalInt.empty());

                if (columns != null) {
                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode.getId(),
                            pageSourceManager,
                            pageProcessor,
                            table,
                            tableCredentials,
                            columns,
                            dynamicFilter,
                            getTypes(projections),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings);
                }

                OperatorFactory operatorFactory = FilterAndProjectOperator.createOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        () -> pageProcessor.apply(dynamicFilter),
                        getTypes(projections),
                        getFilterAndProjectMinOutputPageSize(session),
                        getFilterAndProjectMinOutputPageRowCount(session));

                return new PhysicalOperation(operatorFactory, outputMappings, source);
            }
            catch (TrinoException e) {
                throw e;
            }
            catch (RuntimeException e) {
                if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                    throw new TrinoException(
                            QUERY_EXCEEDED_COMPILER_LIMIT,
                            "Compiler failed. Possible reasons include: the query may have too many or too complex expressions, " +
                                    "or the underlying tables may have too many columns",
                            e);
                }
                throw new TrinoException(COMPILER_ERROR, e);
            }
        }

        @Override
        public PhysicalOperation visitChooseAlternativeNode(ChooseAlternativeNode node, LocalExecutionPlanContext context)
        {
            Function<PlanNode, TableHandle> tableHandleProvider = this::findTableScanForAlternative;

            if (isCacheChooseAlternativeNode(node)) {
                // Load from cache alternative does not have table handle.
                // Cache alternatives have specific ordering and are handled explicitly,
                // therefore table handle is not needed.
                tableHandleProvider = _ -> createCacheTableHandle();
                // when splits are cached dynamic filter needs to be static during split processing
                LoadCachedDataPlanNode loadCachedData = getLoadCachedDataPlanNode(node);
                TableScanNode commonTableScan = node.getOriginalTableScan().tableScanNode();
                Supplier<StaticDynamicFilter> dynamicFilterSupplier = node.getOriginalTableScan().filterPredicate()
                        .map(predicate -> getDynamicFilter(commonTableScan, predicate, context))
                        .map(dynamicFilter -> createStaticDynamicFilterSupplier(ImmutableList.of(dynamicFilter)))
                        .orElse(() -> createStaticDynamicFilter(ImmutableList.of(InternalDynamicFilter.EMPTY)));
                context.setCacheContext(new CacheContext(
                        node.getOriginalTableScan().tableHandle(),
                        loadCachedData,
                        dynamicFilterSupplier));
            }

            ImmutableMap.Builder<TableHandle, PhysicalOperation> alternatives = ImmutableMap.builder();
            Map<Symbol, Integer> outputLayout = null;
            for (PlanNode alternative : node.getSources()) {
                TableHandle tableHandle = tableHandleProvider.apply(alternative);
                PhysicalOperation alternativeOperation = alternative.accept(this, context);
                if (outputLayout == null) {
                    // we need an output layout, we may as well take it from the first alternative.
                    // this is consistent with ChooseAlternativeNode.getOutputSymbols
                    outputLayout = alternativeOperation.getLayout();
                    alternatives.put(tableHandle, alternativeOperation);
                }
                else {
                    checkArgument(outputLayout.equals(alternativeOperation.getLayout()),
                            "All alternatives should have the same layout but %s != %s",
                            outputLayout,
                            alternativeOperation.getLayout());
                    // we don't need channel reordering if layout matches exactly
                    alternatives.put(tableHandle, alternativeOperation);
                }
            }

            return new PhysicalOperation(alternatives.buildOrThrow(), node.getId(), outputLayout);
        }

        private TableHandle findTableScanForAlternative(PlanNode chain)
        {
            return searchFrom(chain)
                    .recurseOnlyWhen(node -> node.getSources().size() < 2)
                    .where(node -> node instanceof TableScanNode)
                    .findFirst()
                    .map(node -> ((TableScanNode) node).getTable())
                    .orElseThrow(() -> new IllegalArgumentException("TableHandle not found / not a node chain"));
        }

        @Override
        public PhysicalOperation visitCacheDataPlanNode(CacheDataPlanNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            return new PhysicalOperation(
                    new CacheDataOperatorFactory(context.getNextOperatorId(), node.getId(), getCacheMaxSplitSize(session).toBytes()),
                    source.getLayout(),
                    source);
        }

        @Override
        public PhysicalOperation visitLoadCachedDataPlanNode(LoadCachedDataPlanNode node, LocalExecutionPlanContext context)
        {
            return new PhysicalOperation(
                    new LoadCachedDataOperatorFactory(context.getNextOperatorId(), node.getId()),
                    makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            PlanNodeId planNodeId = node.getId();
            ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
                columnTypes.add(symbol.type());
            }

            Optional<ConnectorTableCredentials> tableCredentials = context.getTaskContext().getTableCredentials(node.getId());
            if (isGpuExecutionEnabled(session) &&
                    columnTypes.build().stream().allMatch(GpuTypeConversion::isConvertible) &&
                    pageSourceManager.supportsConnectorGpuPageSource(node.getTable().catalogHandle(), node.getTable().connectorHandle())) {
                OperatorFactory operatorFactory = new GpuOperator.SourceFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        pageSourceManager.createPageSourceProvider(node.getTable().catalogHandle()),
                        session,
                        node.getTable(),
                        tableCredentials,
                        columns.build(),
                        DynamicFilter.EMPTY,
                        columnTypes.build());
                return new PhysicalOperation(operatorFactory, makeLayout(node));
            }
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), planNodeId, node.getId(), pageSourceManager, node.getTable(), tableCredentials, columns.build(), columnTypes.build());
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        private Optional<Expression> getStaticFilter(Expression filterExpression)
        {
            DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
            Expression staticFilter = combineConjuncts(extractDynamicFilterResult.getStaticConjuncts());
            if (staticFilter.equals(TRUE)) {
                return Optional.empty();
            }
            return Optional.of(staticFilter);
        }

        private InternalDynamicFilter getDynamicFilter(
                TableScanNode tableScanNode,
                Expression filterExpression,
                LocalExecutionPlanContext context)
        {
            DynamicFilters.ExtractResult extractDynamicFilterResult = extractDynamicFilters(filterExpression);
            List<DynamicFilters.Descriptor> dynamicFilters = extractDynamicFilterResult.getDynamicConjuncts();
            if (dynamicFilters.isEmpty()) {
                return InternalDynamicFilter.EMPTY;
            }

            log.debug("[TableScan] Dynamic filters: %s", dynamicFilters);
            context.registerCoordinatorDynamicFilters(dynamicFilters);
            return context.getDynamicFiltersCollector().createDynamicFilter(
                    dynamicFilters,
                    tableScanNode.getAssignments(),
                    plannerContext);
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            if (node.getRowCount() == 0) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node));
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols());
            PageBuilder pageBuilder = new PageBuilder(node.getRowCount(), outputTypes);
            for (int i = 0; i < node.getRowCount(); i++) {
                // declare position for every row
                pageBuilder.declarePosition();
                // evaluate values for non-empty rows
                if (node.getRows().isPresent()) {
                    Expression row = node.getRows().get().get(i);
                    checkState(row.type() instanceof RowType, "unexpected type of Values row: %s", row.type());
                    // evaluate the literal value
                    SqlRow result = (SqlRow) evaluator.evaluate(row, session, ImmutableMap.of());
                    int rawIndex = result.getRawIndex();
                    for (int j = 0; j < outputTypes.size(); j++) {
                        // divide row into fields
                        Block fieldBlock = result.getRawFieldBlock(j);
                        writeNativeValue(outputTypes.get(j), pageBuilder.getBlockBuilder(j), readNativeValue(outputTypes.get(j), fieldBlock, rawIndex));
                    }
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(symbol.type());
            }

            List<Symbol> unnestSymbols = node.getMappings().stream()
                    .map(UnnestNode.Mapping::getInput)
                    .collect(toImmutableList());

            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(symbol.type());
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(Symbol::type);
            ordinalityType.ifPresent(type -> checkState(type.equals(BIGINT), "Type of ordinalitySymbol must always be BIGINT."));

            List<Integer> replicateChannels = getChannelsForSymbols(node.getReplicateSymbols(), source.getLayout());
            List<Integer> unnestChannels = getChannelsForSymbols(unnestSymbols, source.getLayout());

            // Source channels are always laid out first, followed by the unnested symbols
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getReplicateSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            for (UnnestNode.Mapping mapping : node.getMappings()) {
                for (Symbol unnestedSymbol : mapping.getOutputs()) {
                    outputMappings.put(unnestedSymbol, channel);
                    channel++;
                }
            }

            if (ordinalitySymbol.isPresent()) {
                outputMappings.put(ordinalitySymbol.get(), channel);
                channel++;
            }
            boolean outer = node.getJoinType() == LEFT || node.getJoinType() == FULL;
            OperatorFactory operatorFactory = new UnnestOperator.UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent(),
                    outer);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private Map<Symbol, Integer> makeLayout(PlanNode node)
        {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        private Map<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.buildOrThrow();
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

            SetMultimap<Symbol, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupSymbols()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = ImmutableList.copyOf(node.getLookupSymbols());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (Symbol lookupSymbol : lookupSymbolSchema) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupSymbol);
                checkState(!potentialProbeInputs.isEmpty(), "Must have at least one source from the probe input");
                if (potentialProbeInputs.size() > 1) {
                    overlappingFieldSetsBuilder.add(ImmutableSet.copyOf(potentialProbeInputs));
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = recordSet -> {
                if (!overlappingFieldSets.isEmpty()) {
                    recordSet = new FieldSetFilteringRecordSet(plannerContext.getTypeOperators(), recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<Symbol, Symbol> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinSymbols);

            // Map the index join symbols to the probe key Input
            Multimap<Symbol, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<Symbol, Integer> builder = ImmutableSetMultimap.builder();
            for (Entry<Symbol, Symbol> entry : indexKeyTrace.entrySet()) {
                Symbol indexJoinSymbol = entry.getKey();
                Symbol indexLookupSymbol = entry.getValue();
                builder.putAll(indexLookupSymbol, indexToProbeKeyInput.get(indexJoinSymbol));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<Symbol, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeSymbols.size(); i++) {
                // Duplicate symbols can appear and we only need to take one of the Inputs
                probeKeyLayout.put(probeSymbols.get(i), i);
            }

            // Plan the index source side
            SetMultimap<Symbol, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<Symbol> indexSymbolsNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexSymbols)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(indexSource.getLayout()::get)
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(
                        filterOperatorId,
                        node.getId(),
                        nonLookupInputChannels,
                        nonLookupOutputChannels,
                        indexSource.getTypes(),
                        pageFunctionCompiler,
                        blockTypeOperators));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextPipelineId(),
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getTypes(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceFactory indexLookupSourceFactory = new IndexLookupSourceFactory(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexSource.getTypes(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session),
                    pagesIndexFactory,
                    hashStrategyCompiler,
                    blockTypeOperators);

            indexLookupSourceFactory.setTaskContext(context.taskContext);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    indexLookupSourceFactory,
                    indexLookupSourceFactory.getOutputTypes());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from index side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Entry<Symbol, Integer> entry : indexSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory lookupJoinOperatorFactory;
            OptionalInt totalOperatorsCount = context.getDriverInstanceCount();
            // We use spilling operator since Non-spilling one does not support index lookup sources
            lookupJoinOperatorFactory = switch (node.getType()) {
                case INNER -> spillingJoin(
                        JoinOperatorType.innerJoin(false, false),
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactoryManager,
                        probeSource.getTypes(),
                        probeChannels,
                        Optional.empty(),
                        totalOperatorsCount,
                        unsupportedPartitioningSpillerFactory(),
                        hashCompiler,
                        OptionalInt.empty());
                case SOURCE_OUTER -> spillingJoin(
                        JoinOperatorType.probeOuterJoin(false),
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactoryManager,
                        probeSource.getTypes(),
                        probeChannels,
                        Optional.empty(),
                        totalOperatorsCount,
                        unsupportedPartitioningSpillerFactory(),
                        hashCompiler,
                        OptionalInt.empty());
            };
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.buildOrThrow(), probeSource);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            // Register dynamic filters, allowing the scan operators to wait for the collection completion.
            // Skip dynamic filters that are not used locally (e.g. in case of distributed joins).
            Set<DynamicFilterId> localDynamicFilters = node.getDynamicFilters().keySet().stream()
                    .filter(getConsumedDynamicFilterIds(node.getLeft())::contains)
                    .collect(toImmutableSet());
            context.getDynamicFiltersCollector().register(localDynamicFilters);

            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, localDynamicFilters, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            return switch (node.getType()) {
                case INNER, LEFT, RIGHT, FULL -> createLookupJoin(node, node.getLeft(), leftSymbols, node.getRight(), rightSymbols, localDynamicFilters, context);
            };
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            Expression filterExpression = node.getFilter();
            List<Call> spatialFunctions = extractSupportedSpatialFunctions(filterExpression);
            for (Call spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<Comparison> spatialComparisons = extractSupportedSpatialComparisons(filterExpression);
            for (Comparison spatialComparison : spatialComparisons) {
                if (spatialComparison.operator() == LESS_THAN || spatialComparison.operator() == LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    Expression radius = spatialComparison.right();
                    if (radius instanceof Reference && getSymbolReferences(node.getRight().getOutputSymbols()).contains(radius) || radius instanceof Constant) {
                        Call spatialFunction = (Call) spatialComparison.left();
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialComparison), spatialFunction, Optional.of(radius), Optional.of(spatialComparison.operator()));
                        if (operation.isPresent()) {
                            return operation.get();
                        }
                    }
                }
            }

            throw new VerifyException("No valid spatial relationship found for spatial join");
        }

        private Optional<PhysicalOperation> tryCreateSpatialJoin(
                LocalExecutionPlanContext context,
                SpatialJoinNode node,
                Optional<Expression> filterExpression,
                Call spatialFunction,
                Optional<Expression> radius,
                Optional<Comparison.Operator> comparisonOperator)
        {
            List<Expression> arguments = spatialFunction.arguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof Reference firstSymbol) || !(arguments.get(1) instanceof Reference secondSymbol)) {
                return Optional.empty();
            }

            PlanNode probeNode = node.getLeft();
            Set<Reference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<Reference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            Optional<Symbol> radiusSymbol = Optional.empty();
            OptionalDouble constantRadius = OptionalDouble.empty();
            if (radius.isPresent()) {
                Expression expression = radius.get();
                if (expression instanceof Reference reference) {
                    radiusSymbol = Optional.of(Symbol.from(reference));
                }
                else if (expression instanceof Constant constant) {
                    constantRadius = OptionalDouble.of((Double) constant.value());
                }
                else {
                    throw new IllegalArgumentException("Unexpected expression for radius: " + expression);
                }
            }

            if (probeSymbols.contains(firstSymbol) && buildSymbols.contains(secondSymbol)) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        Symbol.from(firstSymbol),
                        buildNode,
                        Symbol.from(secondSymbol),
                        radiusSymbol,
                        constantRadius,
                        spatialTest(spatialFunction, true, comparisonOperator),
                        filterExpression,
                        context));
            }
            if (probeSymbols.contains(secondSymbol) && buildSymbols.contains(firstSymbol)) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        Symbol.from(secondSymbol),
                        buildNode,
                        Symbol.from(firstSymbol),
                        radiusSymbol,
                        constantRadius,
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<Expression> removeExpressionFromFilter(Expression filter, Expression expression)
        {
            Expression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE));
            return updatedJoinFilter.equals(TRUE) ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(Call call, boolean probeFirst, Optional<Comparison.Operator> comparisonOperator)
        {
            CatalogSchemaFunctionName functionName = call.function().name();
            if (functionName.equals(builtinFunctionName(ST_CONTAINS))) {
                if (probeFirst) {
                    return (buildGeometry, probeGeometry, _) -> probeGeometry.contains(buildGeometry);
                }
                return (buildGeometry, probeGeometry, _) -> buildGeometry.contains(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_WITHIN))) {
                if (probeFirst) {
                    return (buildGeometry, probeGeometry, _) -> probeGeometry.within(buildGeometry);
                }
                return (buildGeometry, probeGeometry, _) -> buildGeometry.within(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_INTERSECTS))) {
                return (buildGeometry, probeGeometry, _) -> buildGeometry.intersects(probeGeometry);
            }
            if (functionName.equals(builtinFunctionName(ST_DISTANCE))) {
                if (comparisonOperator.orElseThrow() == LESS_THAN) {
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                }
                if (comparisonOperator.get() == LESS_THAN_OR_EQUAL) {
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                }
                throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
            }
            throw new UnsupportedOperationException("Unsupported spatial function: " + functionName);
        }

        private Set<Reference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, Set<DynamicFilterId> localDynamicFilters, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int operatorId = buildContext.getNextOperatorId();
            boolean partitioned = !isBuildSideReplicated(node);
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, localDynamicFilters, partitioned);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(
                        operatorId,
                        localDynamicFilter.get(),
                        node,
                        partitioned,
                        buildContext.getDriverInstanceCount().orElse(1) == 1,
                        buildSource);
            }

            context.addDriverFactory(
                    false,
                    new PhysicalOperation(nestedLoopBuildOperatorFactory, ImmutableMap.of(), buildSource),
                    buildContext);

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            List<Integer> probeChannels = getChannelsForSymbols(node.getLeftOutputSymbols(), probeSource.getLayout());
            List<Integer> buildChannels = getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout());

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinBridgeManager, probeChannels, buildChannels);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                OptionalDouble constantRadius,
                SpatialPredicate spatialRelationshipTest,
                Optional<Expression> joinFilter,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            PagesSpatialIndexFactory pagesSpatialIndexFactory = createPagesSpatialIndexFactory(
                    node,
                    buildNode,
                    buildSymbol,
                    radiusSymbol,
                    constantRadius,
                    probeSource.getLayout(),
                    spatialRelationshipTest,
                    joinFilter,
                    context);

            OperatorFactory operator = createSpatialLookupJoin(node, probeNode, probeSource, probeSymbol, pagesSpatialIndexFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), probeSource);
        }

        private OperatorFactory createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                PhysicalOperation probeSource,
                Symbol probeSymbol,
                PagesSpatialIndexFactory pagesSpatialIndexFactory,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> probeNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            Function<Symbol, OptionalInt> probeChannelGetter = channelGetter(probeSource);
            int probeChannel = probeChannelGetter.apply(probeSymbol).orElseThrow();

            OptionalInt partitionChannel = node.getLeftPartitionSymbol()
                    .map(probeChannelGetter)
                    .orElse(OptionalInt.empty());

            return new SpatialJoinOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getType(),
                    probeTypes,
                    probeOutputChannels,
                    probeChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        private PagesSpatialIndexFactory createPagesSpatialIndexFactory(
                SpatialJoinNode node,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                OptionalDouble constantRadius,
                Map<Symbol, Integer> probeLayout,
                SpatialPredicate spatialRelationshipTest,
                Optional<Expression> joinFilter,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> buildNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildLayout));
            Function<Symbol, OptionalInt> buildChannelGetter = channelGetter(buildSource);
            OptionalInt buildChannel = buildChannelGetter.apply(buildSymbol);
            OptionalInt radiusChannel = radiusSymbol.map(buildChannelGetter)
                    .orElse(OptionalInt.empty());

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = joinFilter
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeLayout,
                            buildLayout));

            OptionalInt partitionChannel = node.getRightPartitionSymbol()
                    .map(buildChannelGetter)
                    .orElse(OptionalInt.empty());

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel.orElseThrow(),
                    radiusChannel,
                    constantRadius,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    false,
                    new PhysicalOperation(builderOperatorFactory, ImmutableMap.of(), buildSource),
                    buildContext);

            return builderOperatorFactory.getPagesSpatialIndexFactory();
        }

        private PhysicalOperation createLookupJoin(
                JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Set<DynamicFilterId> localDynamicFilters,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource;
            HashExchangeConstraint priorConstraint = context.getHashExchangeConstraint();
            context.setHashExchangeConstraint(HashExchangeConstraint.HOST_ONLY);
            try {
                probeSource = probeNode.accept(this, context);
            }
            finally {
                context.setHashExchangeConstraint(priorConstraint);
            }

            // Plan build
            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            boolean spillEnabled = isSpillEnabled(session)
                    && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"))
                    && !buildOuter;

            boolean consumedLocalDynamicFilters = !localDynamicFilters.isEmpty();
            List<Type> probeTypes = probeSource.getTypes();
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getLeftOutputSymbols(), probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            OptionalInt totalOperatorsCount = OptionalInt.empty();
            if (spillEnabled) {
                totalOperatorsCount = context.getDriverInstanceCount();
                checkState(totalOperatorsCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");
            }

            // Implementation of hash join operator may only take advantage of output duplicates insensitive joins when:
            // 1. Join is of INNER or LEFT type. For right or full joins all matching build rows must be tagged as visited.
            // 2. Right (build) output symbols are subset of equi-clauses right symbols. If additional build symbols
            //    are produced, then skipping build rows could skip some distinct rows.
            boolean outputSingleMatch = node.isMaySkipOutputDuplicates() &&
                    node.getCriteria().stream()
                            .map(JoinNode.EquiJoinClause::getRight)
                            .collect(toImmutableSet())
                            .containsAll(node.getRightOutputSymbols());

            LocalExecutionPlanContext buildContext = context.createSubContext();
            Optional<PhysicalOperation> gpuOperation = tryPlanGpuLookupJoin(
                    node,
                    buildNode,
                    buildSymbols,
                    probeSource,
                    probeOutputChannels,
                    probeJoinChannels,
                    localDynamicFilters,
                    context,
                    buildContext);
            if (gpuOperation.isPresent()) {
                return gpuOperation.get();
            }
            buildContext.setHashExchangeConstraint(HashExchangeConstraint.HOST_ONLY);
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);

            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildLayout));

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter()
                    .flatMap(filter -> extractSortExpression(ImmutableSet.copyOf(node.getRight().getOutputSymbols()), filter));

            OptionalInt sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(Symbol::from)
                    .map(sortSymbol -> createJoinSourcesLayout(buildLayout, probeSource.getLayout()).get(sortSymbol))
                    .map(OptionalInt::of)
                    .orElse(OptionalInt.empty());

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildLayout))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            List<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            List<Type> buildTypes = buildSource.getTypes();
            int operatorId = buildContext.getNextOperatorId();
            boolean partitioned = !isBuildSideReplicated(node);
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, localDynamicFilters, partitioned);
            if (localDynamicFilter.isPresent()) {
                buildSource = createDynamicFilterSourceOperatorFactory(
                        operatorId,
                        localDynamicFilter.get(),
                        node,
                        partitioned,
                        buildContext.getDriverInstanceCount().orElse(1) == 1,
                        buildSource);
            }

            int taskConcurrency = getTaskConcurrency(session);

            // Wait for build side to be collected before local dynamic filters are
            // consumed by table scan. This way table scan can filter data more efficiently.
            boolean waitForBuild = consumedLocalDynamicFilters;

            OperatorFactory operator;
            if (useSpillingJoinOperator(spillEnabled, session)) {
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = new JoinBridgeManager<>(
                        buildOuter,
                        new PartitionedLookupSourceFactory(
                                buildTypes,
                                buildOutputTypes,
                                buildChannels.stream()
                                        .map(buildTypes::get)
                                        .collect(toImmutableList()),
                                partitionCount,
                                buildOuter,
                                hashCompiler),
                        buildOutputTypes);

                OperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        buildOutputChannels,
                        buildChannels,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories,
                        10_000,
                        pagesIndexFactory,
                        spillEnabled && partitionCount > 1,
                        singleStreamSpillerFactory,
                        incrementalLoadFactorHashArraySizeSupplier(
                                session,
                                // scale load factor in case partition count (and number of hash build operators)
                                // is reduced (e.g. by plan rule) with respect to default task concurrency
                                taskConcurrency / partitionCount));

                context.addDriverFactory(
                        false,
                        new PhysicalOperation(hashBuilderOperatorFactory, ImmutableMap.of(), buildSource),
                        buildContext);

                JoinOperatorType joinType = JoinOperatorType.ofJoinNodeType(node.getType(), outputSingleMatch, waitForBuild);
                operator = spillingJoin(
                        joinType,
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        probeTypes,
                        probeJoinChannels,
                        Optional.of(probeOutputChannels),
                        totalOperatorsCount,
                        partitioningSpillerFactory,
                        hashCompiler,
                        isParallelizeLookupOuterOperator(session)
                                ? OptionalInt.of(partitionCount)
                                : OptionalInt.empty());
            }
            else {
                JoinBridgeManager<io.trino.operator.join.nonspilling.PartitionedLookupSourceFactory> lookupSourceFactory = new JoinBridgeManager<>(
                        buildOuter,
                        new io.trino.operator.join.nonspilling.PartitionedLookupSourceFactory(
                                buildTypes,
                                buildOutputTypes,
                                buildChannels.stream()
                                        .map(buildTypes::get)
                                        .collect(toImmutableList()),
                                partitionCount,
                                buildOuter,
                                hashCompiler),
                        buildOutputTypes);

                OperatorFactory hashBuilderOperatorFactory = new HashBuilderOperator.HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        buildOutputChannels,
                        buildChannels,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories,
                        10_000,
                        pagesIndexFactory,
                        incrementalLoadFactorHashArraySizeSupplier(
                                session,
                                // scale load factor in case partition count (and number of hash build operators)
                                // is reduced (e.g. by plan rule) with respect to default task concurrency
                                taskConcurrency / partitionCount));

                context.addDriverFactory(
                        false,
                        new PhysicalOperation(hashBuilderOperatorFactory, ImmutableMap.of(), buildSource),
                        buildContext);

                JoinOperatorType joinType = JoinOperatorType.ofJoinNodeType(node.getType(), outputSingleMatch, waitForBuild);
                operator = join(
                        joinType,
                        context.getNextOperatorId(),
                        node.getId(),
                        lookupSourceFactory,
                        node.getFilter().isPresent(),
                        probeTypes,
                        probeJoinChannels,
                        Optional.of(probeOutputChannels),
                        hashCompiler,
                        isParallelizeLookupOuterOperator(session)
                                ? OptionalInt.of(partitionCount)
                                : OptionalInt.empty());
            }

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.buildOrThrow(), probeSource);
        }

        @Override
        public PhysicalOperation visitDynamicFilterSource(DynamicFilterSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(
                    !node.getDynamicFilters().isEmpty(),
                    "Dynamic filters cannot be empty in DynamicFilterSourceNode");
            log.debug("[DynamicFilterSource] Dynamic filters: %s", node.getDynamicFilters());
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<DynamicFilterId, Integer> dynamicFilterChannels = node.getDynamicFilters().entrySet().stream()
                    .collect(toImmutableMap(
                            // Dynamic filter ID
                            Entry::getKey,
                            // Build-side channel index
                            entry -> {
                                Symbol buildSymbol = entry.getValue();
                                int buildChannelIndex = node.getOutputSymbols().indexOf(buildSymbol);
                                verify(buildChannelIndex >= 0);
                                return buildChannelIndex;
                            }));
            Map<DynamicFilterId, Type> dynamicFilterChannelTypes = dynamicFilterChannels.entrySet().stream()
                    .collect(toImmutableMap(
                            Entry::getKey,
                            entry -> source.getTypes().get(entry.getValue())));

            TaskContext taskContext = context.getTaskContext();
            LocalDynamicFilterConsumer dynamicFilterSourceConsumer = new LocalDynamicFilterConsumer(
                    dynamicFilterChannels,
                    dynamicFilterChannelTypes,
                    // In fault-tolerant execution, all tasks need to collect dynamic filters even if the join has
                    // broadcast distribution type because the collection takes place before the remote exchange
                    ImmutableList.of(taskContext::updateDomains),
                    getDynamicFilteringMaxSizePerOperator(false));
            return createDynamicFilterSourceOperatorFactory(
                    context.getNextOperatorId(),
                    dynamicFilterSourceConsumer,
                    node,
                    false,
                    false,
                    source);
        }

        private PhysicalOperation createDynamicFilterSourceOperatorFactory(
                int operatorId,
                LocalDynamicFilterConsumer dynamicFilter,
                PlanNode node,
                boolean partitioned,
                boolean isBuildSideSingle,
                PhysicalOperation buildSource)
        {
            List<DynamicFilterSourceOperator.Channel> filterBuildChannels = dynamicFilter.getBuildChannels().entrySet().stream()
                    .map(entry -> {
                        DynamicFilterId filterId = entry.getKey();
                        int index = entry.getValue();
                        Type type = buildSource.getTypes().get(index);
                        return new DynamicFilterSourceOperator.Channel(filterId, type, index);
                    })
                    .collect(toImmutableList());
            int taskConcurrency = getTaskConcurrency(session);
            return new PhysicalOperation(
                    new DynamicFilterSourceOperatorFactory(
                            operatorId,
                            node.getId(),
                            dynamicFilter,
                            filterBuildChannels,
                            multipleIf(getDynamicFilteringMaxDistinctValuesPerDriver(partitioned), taskConcurrency, isBuildSideSingle),
                            multipleIf(getBloomFilterMaxDistinctValuesPerDriver(partitioned), taskConcurrency, isBuildSideSingle),
                            multipleIf(getDynamicFilteringMaxSizePerDriver(partitioned), taskConcurrency, isBuildSideSingle),
                            typeOperators),
                    buildSource.getLayout(),
                    buildSource);
        }

        private int multipleIf(int value, int multiplier, boolean shouldMultiply)
        {
            return shouldMultiply ? value * multiplier : value;
        }

        private DataSize multipleIf(DataSize value, int multiplier, boolean shouldMultiply)
        {
            return shouldMultiply ? DataSize.ofBytes(value.toBytes() * multiplier) : value;
        }

        private Optional<LocalDynamicFilterConsumer> createDynamicFilter(
                PhysicalOperation buildSource,
                JoinNode node,
                LocalExecutionPlanContext context,
                Set<DynamicFilterId> localDynamicFilters,
                boolean partitioned)
        {
            Set<DynamicFilterId> coordinatorDynamicFilters = getCoordinatorDynamicFilters(node.getDynamicFilters().keySet(), node, context.getTaskId());
            Set<DynamicFilterId> collectedDynamicFilters = ImmutableSet.<DynamicFilterId>builder()
                    .addAll(localDynamicFilters)
                    .addAll(coordinatorDynamicFilters)
                    .build();
            if (collectedDynamicFilters.isEmpty()) {
                return Optional.empty();
            }
            log.debug("[Join] Dynamic filters: %s", node.getDynamicFilters());
            ImmutableList.Builder<Consumer<Map<DynamicFilterId, DynamicFilterDomain>>> collectors = ImmutableList.builder();
            TaskContext taskContext = context.getTaskContext();
            if (!localDynamicFilters.isEmpty()) {
                collectors.add(taskContext::addDynamicFilter);
            }
            if (!coordinatorDynamicFilters.isEmpty()) {
                collectors.add(getCoordinatorDynamicFilterDomainsCollector(taskContext, coordinatorDynamicFilters));
            }
            LocalDynamicFilterConsumer filterConsumer = LocalDynamicFilterConsumer.create(
                    node,
                    buildSource.getTypes(),
                    collectedDynamicFilters,
                    collectors.build(),
                    getDynamicFilteringMaxSizePerOperator(partitioned));

            return Optional.of(filterConsumer);
        }

        private JoinFilterFunctionFactory compileJoinFilterFunction(
                Expression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);

            return joinFilterFunctionCompiler.compileJoinFilterFunction(filterExpression, joinSourcesLayout, buildLayout.size());
        }

        private Map<Symbol, Integer> createJoinSourcesLayout(Map<Symbol, Integer> lookupSourceLayout, Map<Symbol, Integer> probeSourceLayout)
        {
            ImmutableMap.Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            joinSourcesLayout.putAll(lookupSourceLayout);
            for (Entry<Symbol, Integer> probeLayoutEntry : probeSourceLayout.entrySet()) {
                joinSourcesLayout.put(probeLayoutEntry.getKey(), probeLayoutEntry.getValue() + lookupSourceLayout.size());
            }
            return joinSourcesLayout.buildOrThrow();
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            boolean isLocalDynamicFilter = node.getDynamicFilterId()
                    .map(filterId -> getConsumedDynamicFilterIds(node.getSource()).contains(filterId))
                    .orElse(false);
            boolean isCoordinatorDynamicFilter = node.getDynamicFilterId()
                    .map(filterId -> !getCoordinatorDynamicFilters(ImmutableSet.of(filterId), node, context.getTaskId()).isEmpty())
                    .orElse(false);
            if (isLocalDynamicFilter) {
                // Register locally if the table scan is on the same node (e.g., in case of broadcast semi-joins)
                context.getDynamicFiltersCollector().register(ImmutableSet.of(node.getDynamicFilterId().get()));
            }

            // Plan probe
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();

            // GPU path (dynamic filters not yet supported in GPU semi-join)
            if (!isLocalDynamicFilter && !isCoordinatorDynamicFilter) {
                Optional<PhysicalOperation> gpuOp = tryPlanGpuSemiJoin(node, probeSource, context, buildContext);
                if (gpuOp.isPresent()) {
                    return gpuOp.get();
                }
            }

            // Plan build
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);
            checkArgument(partitionCount == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            int operatorId = buildContext.getNextOperatorId();
            if (isLocalDynamicFilter || isCoordinatorDynamicFilter) {
                // Add a DynamicFilterSourceOperatorFactory to build operator factories
                DynamicFilterId filterId = node.getDynamicFilterId().get();
                log.debug("[Semi-join] Dynamic filter: %s", filterId);
                ImmutableList.Builder<Consumer<Map<DynamicFilterId, DynamicFilterDomain>>> collectors = ImmutableList.builder();
                TaskContext taskContext = context.getTaskContext();
                if (isLocalDynamicFilter) {
                    collectors.add(taskContext::addDynamicFilter);
                }
                if (isCoordinatorDynamicFilter) {
                    collectors.add(getCoordinatorDynamicFilterDomainsCollector(taskContext, ImmutableSet.of(filterId)));
                }
                boolean partitioned = !isBuildSideReplicated(node);
                LocalDynamicFilterConsumer filterConsumer = new LocalDynamicFilterConsumer(
                        ImmutableMap.of(filterId, buildChannel),
                        ImmutableMap.of(filterId, buildSource.getTypes().get(buildChannel)),
                        collectors.build(),
                        getDynamicFilteringMaxSizePerOperator(partitioned));
                buildSource = new PhysicalOperation(
                        new DynamicFilterSourceOperatorFactory(
                                operatorId,
                                node.getId(),
                                filterConsumer,
                                ImmutableList.of(new DynamicFilterSourceOperator.Channel(filterId, buildSource.getTypes().get(buildChannel), buildChannel)),
                                getDynamicFilteringMaxDistinctValuesPerDriver(partitioned),
                                getBloomFilterMaxDistinctValuesPerDriver(partitioned),
                                getDynamicFilteringMaxSizePerDriver(partitioned),
                                typeOperators),
                        buildSource.getLayout(),
                        buildSource);
            }

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    10_000,
                    joinCompiler,
                    typeOperators);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    false,
                    new PhysicalOperation(setBuilderOperatorFactory, ImmutableMap.of(), buildSource),
                    buildContext);

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .buildOrThrow();

            OperatorFactory operator = HashSemiJoinOperator.createOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel);
            return new PhysicalOperation(operator, outputMappings, probeSource);
        }

        private static Set<DynamicFilterId> getCoordinatorDynamicFilters(Set<DynamicFilterId> dynamicFilters, PlanNode node, TaskId taskId)
        {
            if (!isBuildSideReplicated(node) || taskId.partitionId() == 0) {
                // replicated dynamic filters are collected by single stage task only
                return dynamicFilters;
            }

            return ImmutableSet.of();
        }

        private static Consumer<Map<DynamicFilterId, DynamicFilterDomain>> getCoordinatorDynamicFilterDomainsCollector(TaskContext taskContext, Set<DynamicFilterId> coordinatorDynamicFilters)
        {
            return domains -> taskContext.updateDomains(
                    domains.entrySet().stream()
                            .filter(entry -> coordinatorDynamicFilters.contains(entry.getKey()))
                            .collect(toImmutableMap(Entry::getKey, Entry::getValue)));
        }

        private Optional<PhysicalOperation> tryPlanGpuSemiJoin(
                SemiJoinNode node,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context,
                LocalExecutionPlanContext buildContext)
        {
            if (!isGpuExecutionEnabled(session)) {
                return Optional.empty();
            }

            if (!probeSource.getTypes().stream().allMatch(GpuTypeConversion::isConvertible) ||
                    !GpuTypeConversion.isConvertible(node.getFilteringSourceJoinSymbol().type())) {
                return Optional.empty();
            }

            buildContext.setDriverInstanceCount(1);
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            GpuSemiJoinSetSupplier setSupplier = new GpuSemiJoinSetSupplier();

            PhysicalOperation joinBuild = addGpuOperation(
                    new GpuSemiJoinBuild.Factory(setSupplier, buildChannel),
                    ImmutableList.of(),
                    buildSource,
                    ImmutableMap.of(),
                    buildContext,
                    node.getId());

            // For the last operator, Driver does not call getOutput(), only addInput() (guarded by needsInput()) and finish() (when input exhausted).
            // This means that the sink operator can never declare "I temporarily do not want more input", which is incompatible with GPU's operations
            // contract such as BufferPages. We're a dummy operator so that Driver calls getOutput() allowing the build side to do its work.
            joinBuild = new PhysicalOperation(
                    new SentinelSinkOperator.Factory(buildContext.getNextOperatorId(), node.getId()),
                    ImmutableMap.of(),
                    joinBuild);

            context.addDriverFactory(false, joinBuild, buildContext);

            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .buildOrThrow();

            List<Type> outputTypes = ImmutableList.<Type>builder()
                    .addAll(probeSource.getTypes())
                    .add(BOOLEAN)
                    .build();

            return Optional.of(addGpuOperation(
                    new GpuSemiJoin.Factory(setSupplier, probeChannel),
                    outputTypes,
                    probeSource,
                    outputMappings,
                    context,
                    node.getId()));
        }

        @Override
        public PhysicalOperation visitRefreshMaterializedView(RefreshMaterializedViewNode node, LocalExecutionPlanContext context)
        {
            context.setDriverInstanceCount(1);
            OperatorFactory operatorFactory = new RefreshMaterializedViewOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, node.getViewName());
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            int maxWriterCount = getWriterCount(
                    session,
                    node.getTarget().getWriterScalingOptions(metadata, session),
                    node.getPartitioningScheme(),
                    node.getSource());
            context.setDriverInstanceCount(maxWriterCount);
            context.taskContext.setMaxWriterCount(maxWriterCount);

            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        DataSize.ofBytes(0),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty());
            }).orElseGet(() -> new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    context.getTaskContext().getTableCredentials(node.getId()),
                    inputChannels,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), source);
        }

        @Override
        public PhysicalOperation visitStatisticsWriterNode(StatisticsWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StatisticAggregationsDescriptor<Integer> descriptor = node.getDescriptor().map(symbol -> source.getLayout().get(symbol));

            OperatorFactory operatorFactory = new StatisticsWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    computedStatistics -> metadata.finishStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsHandle) node.getTarget()).getHandle(), computedStatistics),
                    node.isRowCountEnabled(),
                    descriptor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            FINAL,
                            0,
                            outputMapping,
                            source,
                            context);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        FINAL,
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        DataSize.ofBytes(0),
                        context,
                        0,
                        outputMapping,
                        200,
                        // final aggregation ignores partial pre-aggregation memory limit
                        Optional.empty());
            }).orElseGet(() -> new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.buildOrThrow();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(aggregationOutput::get))
                    .orElseGet(StatisticAggregationsDescriptor::empty);

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    tableExecuteContextManager,
                    shouldOutputRowCount(node),
                    session);
            Map<Symbol, Integer> layout = makeLayoutFromOutputSymbols(node.getOutputSymbols());

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitSimpleTableExecuteNode(SimpleTableExecuteNode node, LocalExecutionPlanContext context)
        {
            context.setDriverInstanceCount(1);
            SimpleTableExecuteOperatorOperatorFactory operatorFactory =
                    new SimpleTableExecuteOperatorOperatorFactory(
                            context.getNextOperatorId(),
                            node.getId(),
                            metadata,
                            session,
                            node.getExecuteHandle(),
                            getSymbolTypes(node.getOutputSymbols()));

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableExecute(TableExecuteNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            int maxWriterCount = getWriterCount(
                    session,
                    node.getTarget().getWriterScalingOptions(metadata, session),
                    node.getPartitioningScheme(),
                    node.getSource());
            context.setDriverInstanceCount(maxWriterCount);
            context.taskContext.setMaxWriterCount(maxWriterCount);

            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    context.getTaskContext().getTableCredentials(node.getId()),
                    inputChannels,
                    session,
                    new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()), // statistics are not calculated
                    getSymbolTypes(node.getOutputSymbols()));

            return new PhysicalOperation(operatorFactory, outputMapping.buildOrThrow(), source);
        }

        private int getWriterCount(Session session, WriterScalingOptions connectorScalingOptions, Optional<PartitioningScheme> partitioningScheme, PlanNode source)
        {
            // This check is required because we don't know which writer count to use when exchange is
            // single distribution. It could be possible that when scaling is enabled, a single distribution is
            // selected for partitioned write using "task_max_writer_count". However, we can't say for sure
            // whether this single distribution comes from unpartitioned or partitioned writer count.
            if (isSingleGatheringExchange(source)) {
                return 1;
            }

            if (partitioningScheme.isPresent()) {
                // The default value of partitioned writer count is 2 * number_of_cores (capped to 64) which is high
                // enough to use it for cases with or without scaling enabled. Additionally, it doesn't lead
                // to too many small files when scaling is disabled because single partition will be written by
                // a single writer only.
                int partitionedWriterCount = getTaskMaxWriterCount(session);
                if (isLocalScaledWriterExchange(source)) {
                    partitionedWriterCount = connectorScalingOptions.perTaskMaxScaledWriterCount()
                            .map(writerCount -> min(writerCount, getTaskMaxWriterCount(session)))
                            .orElse(getTaskMaxWriterCount(session));
                }
                return getPartitionedWriterCountBasedOnMemory(partitionedWriterCount, session);
            }

            int unpartitionedWriterCount = getTaskMinWriterCount(session);
            if (isLocalScaledWriterExchange(source)) {
                unpartitionedWriterCount = connectorScalingOptions.perTaskMaxScaledWriterCount()
                        .map(writerCount -> min(writerCount, getTaskMaxWriterCount(session)))
                        .orElse(getTaskMaxWriterCount(session));
            }
            // Consider memory while calculating writer count.
            return min(unpartitionedWriterCount, getMaxWritersBasedOnMemory(session));
        }

        private boolean isSingleGatheringExchange(PlanNode node)
        {
            Optional<PlanNode> result = searchFrom(node)
                    .where(planNode -> planNode instanceof ExchangeNode)
                    .findFirst();

            return result.isPresent()
                    && result.get() instanceof ExchangeNode exchangeNode
                    && exchangeNode.getPartitioningScheme().getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION);
        }

        @Override
        public PhysicalOperation visitMergeWriter(MergeWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            int maxWriterCount = getWriterCount(
                    session,
                    node.getTarget().getWriterScalingOptions(metadata, session),
                    node.getPartitioningScheme(),
                    node.getSource());
            context.setDriverInstanceCount(maxWriterCount);
            context.taskContext.setMaxWriterCount(maxWriterCount);

            PhysicalOperation source = node.getSource().accept(this, context);

            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(node.getProjectedSymbols(), source.getLayout());

            Optional<ConnectorTableCredentials> tableCredentials = context.getTaskContext().getTableCredentials(node.getId());
            OperatorFactory operatorFactory = new MergeWriterOperatorFactory(context.getNextOperatorId(), node.getId(), pageSinkManager, node.getTarget(), tableCredentials, session, pagePreprocessor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitMergeProcessor(MergeProcessorNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Map<Symbol, Integer> nodeLayout = makeLayout(node);
            Map<Symbol, Integer> sourceLayout = makeLayout(node.getSource());
            int rowIdChannel = sourceLayout.get(node.getRowIdSymbol());
            int mergeRowChannel = sourceLayout.get(node.getMergeRowSymbol());

            List<Integer> redistributionColumns = node.getRedistributionColumnSymbols().stream()
                    .map(nodeLayout::get)
                    .collect(toImmutableList());
            List<Integer> dataColumnChannels = node.getDataColumnSymbols().stream()
                    .map(nodeLayout::get)
                    .collect(toImmutableList());

            List<Symbol> expectedLayout = node.getSource().getOutputSymbols();
            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());

            OperatorFactory operatorFactory = MergeProcessorOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getTarget().getMergeParadigmAndTypes(),
                    rowIdChannel,
                    mergeRowChannel,
                    redistributionColumns,
                    dataColumnChannels,
                    pagePreprocessor);
            return new PhysicalOperation(operatorFactory, nodeLayout, source);
        }

        @Override
        public PhysicalOperation visitTableDelete(TableDeleteNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableMutationOperatorFactory(context.getNextOperatorId(), node.getId(), () -> metadata.executeDelete(session, node.getTarget()));

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitTableUpdate(TableUpdateNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new TableMutationOperatorFactory(context.getNextOperatorId(), node.getId(), () -> metadata.executeUpdate(session, node.getTarget()));

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("Union node should not be present in a local execution plan");
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes());
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = AssignUniqueIdOperator.createOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private boolean isLocalScaledWriterExchange(PlanNode node)
        {
            Optional<PlanNode> result = searchFrom(node)
                    .where(planNode -> planNode instanceof ExchangeNode exchangeNode && exchangeNode.getScope() == LOCAL)
                    .findFirst();

            return result.isPresent()
                    && result.get() instanceof ExchangeNode exchangeNode
                    && exchangeNode.getPartitioningScheme().getPartitioning().getHandle().isScaleWriters();
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node);
            LocalExchange localExchange = new LocalExchange(
                    partitionFunctionProvider,
                    session,
                    operatorsCount,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    node.getPartitioningScheme().getBucketCount(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    positionsAppenderFactory,
                    types,
                    maxLocalExchangeBufferSize,
                    hashCompiler,
                    getWriterScalingMinDataProcessed(session),
                    () -> context.getTaskContext().getQueryMemoryReservation().toBytes());

            List<Symbol> expectedLayout = getOnlyElement(node.getInputs());
            Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());
            context.addDriverFactory(
                    false,
                    new PhysicalOperation(
                            new LocalExchangeSinkOperatorFactory(
                                    localExchange.createSinkFactory(),
                                    subContext.getNextOperatorId(),
                                    node.getId(),
                                    pagePreprocessor),
                            ImmutableMap.of(),
                            source),
                    subContext);
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            Map<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForSymbols(orderingScheme.orderBy(), layout);
            List<SortOrder> orderings = orderingScheme.orderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    localExchange,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout);
        }

        private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session);
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node);
            List<Integer> partitionChannels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            List<Type> partitionChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());

            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));
            }

            Optional<PhysicalOperation> gpuOperation = tryPlanGpuLocalExchange(node, context, driverInstanceCount, driverFactoryParametersList);
            if (gpuOperation.isPresent()) {
                return gpuOperation.get();
            }

            LocalExchange localExchange = new LocalExchange(
                    partitionFunctionProvider,
                    session,
                    driverInstanceCount,
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    node.getPartitioningScheme().getBucketCount(),
                    partitionChannels,
                    partitionChannelTypes,
                    positionsAppenderFactory,
                    types,
                    maxLocalExchangeBufferSize,
                    hashCompiler,
                    getWriterScalingMinDataProcessed(session),
                    () -> context.getTaskContext().getQueryMemoryReservation().toBytes());
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<Symbol> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLoadedLayoutProcessor(expectedLayout, source.getLayout());

                context.addDriverFactory(
                        false,
                        new PhysicalOperation(
                                new LocalExchangeSinkOperatorFactory(
                                        localExchange.createSinkFactory(),
                                        subContext.getNextOperatorId(),
                                        node.getId(),
                                        pagePreprocessor),
                                ImmutableMap.of(),
                                source),
                        subContext);
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchange.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchange), makeLayout(node));
        }

        private Optional<PhysicalOperation> tryPlanGpuLocalExchange(
                ExchangeNode node,
                LocalExecutionPlanContext context,
                int driverInstanceCount,
                List<DriverFactoryParameters> driverFactoryParameters)
        {
            if (!isGpuExecutionEnabled(session) || !isGpuLocalExchangeEligible(node)) {
                return Optional.empty();
            }
            PartitioningHandle partitioning = node.getPartitioningScheme().getPartitioning().getHandle();
            if (partitioning.equals(FIXED_HASH_DISTRIBUTION) && context.getHashExchangeConstraint() == HashExchangeConstraint.HOST_ONLY) {
                log.debug("Could not plan local exchange for GPU execution: paired HASH LE with non-GPU lookup join consumer");
                return Optional.empty();
            }
            for (DriverFactoryParameters parameters : driverFactoryParameters) {
                List<OperatorFactory> tail = parameters.getSource().getPipelineTail();
                // ChooseAlternativeNode is pushed to the top of a source-stage pipeline, leaving
                // an empty shared tail; conservatively reject rather than inspect each alternative.
                if (tail.isEmpty()) {
                    log.debug("Could not plan local exchange for GPU execution: upstream pipeline is a plan alternative");
                    return Optional.empty();
                }
                OperatorFactory tailOperatorFactory = tail.getLast();
                if (!(tailOperatorFactory instanceof GpuOperator.BaseFactory)) {
                    log.debug("Could not plan local exchange for GPU execution: upstream pipeline ends in %s, not GpuOperator", tailOperatorFactory);
                    return Optional.empty();
                }
            }

            List<Type> outputTypes = getSourceOperatorTypes(node);
            int[] partitionChannels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .mapToInt(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .toArray();

            GpuLocalExchange exchange = new GpuLocalExchange(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    driverInstanceCount,
                    partitionChannels,
                    gpuLocalExchangeBufferSize.toBytes());

            for (int sourceIndex = 0; sourceIndex < driverFactoryParameters.size(); sourceIndex++) {
                DriverFactoryParameters parameters = driverFactoryParameters.get(sourceIndex);
                PhysicalOperation source = parameters.getSource();
                LocalExecutionPlanContext subContext = parameters.getSubContext();
                // LOCAL ExchangeNode inputs match the source output channels for the partitioning
                // handles we accept; fail loudly if a future plan shape breaks this.
                List<Symbol> expectedInputs = node.getInputs().get(sourceIndex);
                Map<Symbol, Integer> sourceLayout = source.getLayout();
                for (int channel = 0; channel < expectedInputs.size(); channel++) {
                    Symbol symbol = expectedInputs.get(channel);
                    Integer actualChannel = sourceLayout.get(symbol);
                    verify(actualChannel != null && actualChannel == channel,
                            "GPU local exchange requires identity input layout (source %s, channel %s, symbol %s, sourceLayout %s)",
                            sourceIndex,
                            channel,
                            symbol,
                            sourceLayout);
                }

                GpuLocalExchange.GpuLocalExchangeSinkFactory sinkFactory = exchange.createSinkFactory();
                PhysicalOperation pipelineWithSink = addGpuOperation(
                        new GpuLocalExchangeWriter.Factory(sinkFactory),
                        outputTypes,
                        source,
                        source.getLayout(),
                        subContext,
                        node.getId());
                // For the last operator, Driver does not call getOutput(), only addInput() (guarded by needsInput()) and finish() (when input exhausted).
                // This means that the sink operator can never declare "I temporarily do not want more input", which is incompatible with GPU's operations
                // contract such as BufferPages. We're a dummy operator so that Driver calls getOutput() allowing the build side to do its work.
                PhysicalOperation sinkDriver = new PhysicalOperation(
                        new SentinelSinkOperator.Factory(subContext.getNextOperatorId(), node.getId()),
                        pipelineWithSink.getLayout(),
                        pipelineWithSink);
                context.addDriverFactory(false, sinkDriver, subContext);
            }

            context.setInputDriver(false);
            verify(context.getDriverInstanceCount().getAsInt() == exchange.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            GpuOperator.Factory sourceFactory = new GpuOperator.Factory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchange.readerSourceFactory(),
                    outputTypes);

            return Optional.of(new PhysicalOperation(sourceFactory, makeLayout(node)));
        }

        @Override
        public PhysicalOperation visitAdaptivePlanNode(AdaptivePlanNode node, LocalExecutionPlanContext context)
        {
            return node.getCurrentPlan().accept(this, context);
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node)
        {
            return getSymbolTypes(node.getOutputSymbols());
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols)
        {
            return symbols.stream()
                    .map(Symbol::type)
                    .collect(toImmutableList());
        }

        private AggregatorFactory buildAggregatorFactory(
                PhysicalOperation source,
                Aggregation aggregation,
                Step step)
        {
            List<Integer> argumentChannels = new ArrayList<>();
            for (Expression argument : aggregation.getArguments()) {
                if (!(argument instanceof Lambda)) {
                    Symbol argumentSymbol = Symbol.from(argument);
                    argumentChannels.add(source.getLayout().get(argumentSymbol));
                }
            }

            ResolvedFunction resolvedFunction = aggregation.getResolvedFunction();
            AggregationImplementation aggregationImplementation = plannerContext.getFunctionManager().getAggregationImplementation(aggregation.getResolvedFunction());
            AccumulatorFactory accumulatorFactory = uncheckedCacheGet(
                    accumulatorFactoryCache,
                    new FunctionKey(resolvedFunction.functionId(), resolvedFunction.signature()),
                    () -> generateAccumulatorFactory(
                            resolvedFunction.signature(),
                            aggregationImplementation,
                            resolvedFunction.functionNullability(),
                            specializeAggregationLoops));

            if (aggregation.isDistinct()) {
                accumulatorFactory = new DistinctAccumulatorFactory(
                        accumulatorFactory,
                        argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList()),
                        hashStrategyCompiler,
                        session);
            }

            if (aggregation.getOrderingScheme().isPresent()) {
                List<Integer> inputArgumentChannels = range(0, argumentChannels.size())
                        .boxed()
                        .collect(toImmutableList());

                OrderingScheme orderingScheme = aggregation.getOrderingScheme().get();
                List<Symbol> sortKeys = orderingScheme.orderBy();

                List<SortOrder> sortOrders = sortKeys.stream()
                        .map(orderingScheme::ordering)
                        .collect(toImmutableList());

                List<Integer> inputOrderByChannels = new ArrayList<>();
                for (int orderByChannel : getChannelsForSymbols(sortKeys, source.getLayout())) {
                    int inputChannel = argumentChannels.indexOf(orderByChannel);
                    if (inputChannel < 0) {
                        inputChannel = argumentChannels.size();
                        argumentChannels.add(orderByChannel);
                    }
                    inputOrderByChannels.add(inputChannel);
                }

                accumulatorFactory = new OrderedAccumulatorFactory(
                        accumulatorFactory,
                        argumentChannels.stream()
                                .map(channel -> source.getTypes().get(channel))
                                .collect(toImmutableList()),
                        inputArgumentChannels,
                        inputOrderByChannels,
                        sortOrders,
                        pagesIndexFactory);
            }

            List<Type> intermediateTypes = aggregationImplementation.getAccumulatorStateDescriptors().stream()
                    .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                    .collect(toImmutableList());
            Type intermediateType = (intermediateTypes.size() == 1) ? getOnlyElement(intermediateTypes) : RowType.anonymous(intermediateTypes);
            Type finalType = resolvedFunction.signature().getReturnType();

            OptionalInt maskChannel = aggregation.getMask().stream()
                    .mapToInt(value -> source.getLayout().get(value))
                    .findAny();

            List<Lambda> lambdas = aggregation.getArguments().stream()
                    .filter(Lambda.class::isInstance)
                    .map(Lambda.class::cast)
                    .collect(toImmutableList());
            List<FunctionType> functionTypes = resolvedFunction.signature().getArgumentTypes().stream()
                    .filter(FunctionType.class::isInstance)
                    .map(FunctionType.class::cast)
                    .collect(toImmutableList());
            List<Supplier<Object>> lambdaProviders = makeLambdaProviders(lambdas, aggregationImplementation.getLambdaInterfaces(), functionTypes);

            return new AggregatorFactory(
                    accumulatorFactory,
                    step,
                    intermediateType,
                    finalType,
                    argumentChannels,
                    maskChannel,
                    !aggregation.isDistinct() && aggregation.getOrderingScheme().isEmpty(),
                    lambdaProviders);
        }

        private List<Supplier<Object>> makeLambdaProviders(List<Lambda> lambdas, List<Class<?>> lambdaInterfaces, List<FunctionType> functionTypes)
        {
            List<Supplier<Object>> lambdaProviders = new ArrayList<>();
            if (!lambdas.isEmpty()) {
                verify(lambdas.size() == functionTypes.size());
                verify(lambdas.size() == lambdaInterfaces.size());

                for (int i = 0; i < lambdas.size(); i++) {
                    Lambda lambdaExpression = lambdas.get(i);
                    FunctionType functionType = functionTypes.get(i);

                    // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                    // which requires the types of all sub-expressions.
                    //
                    // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                    // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                    //
                    // This does not work here since the function call representation in final aggregation node
                    // is currently a hack: it takes intermediate type as input, and may not be a valid
                    // function call in Trino.
                    //
                    // TODO: Once the final aggregation function call representation is fixed,
                    // the same mechanism in project and filter expression should be used here.
                    verify(lambdaExpression.arguments().size() == functionType.getArgumentTypes().size());

                    Class<? extends Supplier<Object>> lambdaProviderClass = compileLambdaProvider(lambdaExpression, plannerContext.getFunctionManager(), metadata, plannerContext.getTypeManager(), maxMethodComplexity, lambdaInterfaces.get(i));
                    try {
                        lambdaProviders.add(lambdaProviderClass.getConstructor(ConnectorSession.class).newInstance(session.toConnectorSession()));
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return lambdaProviders;
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            AggregationOperatorFactory operatorFactory = createAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getStep(),
                    0,
                    outputMappings,
                    source,
                    context);
            return new PhysicalOperation(operatorFactory, outputMappings.buildOrThrow(), source);
        }

        private AggregationOperatorFactory createAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Step step,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                PhysicalOperation source,
                LocalExecutionPlanContext context)
        {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AggregatorFactory> aggregatorFactories = ImmutableList.builder();
            for (Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();
                aggregatorFactories.add(buildAggregatorFactory(source, aggregation, step));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }
            return new AggregationOperatorFactory(context.getNextOperatorId(), planNodeId, aggregatorFactories.build());
        }

        private PhysicalOperation planGroupByAggregation(
                AggregationNode node,
                PhysicalOperation source,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getGlobalGroupingSets(),
                    node.getGroupingKeys(),
                    node.getStep(),
                    node.getGroupIdSymbol(),
                    source,
                    node.hasDefaultOutput(),
                    spillEnabled,
                    node.isStreamable(),
                    unspillMemoryLimit,
                    context,
                    0,
                    mappings,
                    10_000,
                    Optional.of(maxPartialAggregationMemorySize));
            return new PhysicalOperation(operatorFactory, mappings.buildOrThrow(), source);
        }

        private Optional<PhysicalOperation> tryPlanGpuAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            if (!isGpuExecutionEnabled(session)) {
                return Optional.empty();
            }
            return GpuAggregationCompiler.compile(node, source.getLayout())
                    .map(compileResult -> addGpuOperations(
                            compileResult.stages(),
                            compileResult.finalOutputTypes(),
                            source,
                            makeLayout(node),
                            context,
                            node.getId()));
        }

        private Optional<PhysicalOperation> tryPlanGpuLookupJoin(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                PhysicalOperation probeSource,
                List<Integer> probeOutputChannels,
                List<Integer> probeJoinChannels,
                Set<DynamicFilterId> localDynamicFilters,
                LocalExecutionPlanContext context,
                LocalExecutionPlanContext buildContext)
        {
            if (!isGpuExecutionEnabled(session)) {
                return Optional.empty();
            }

            GpuLookupJoin.JoinType joinType;
            switch (node.getType()) {
                case INNER -> {
                    joinType = GpuLookupJoin.JoinType.INNER;
                }
                case LEFT -> {
                    joinType = GpuLookupJoin.JoinType.LEFT;
                }
                default -> {
                    log.debug("Could not convert join type for GPU execution: %s", node.getType());
                    return Optional.empty();
                }
            }

            // Must have at least one equi-clause
            if (node.getCriteria().isEmpty()) {
                log.debug("Could not convert join without equi-criteria for GPU execution, join type: %s", node.getType());
                return Optional.empty();
            }

            // Every probe and build column must be GPU-convertible (CopyToDevice copies all of them).
            if (!probeSource.getTypes().stream().allMatch(GpuTypeConversion::isConvertible)) {
                return Optional.empty();
            }
            if (!buildNode.getOutputSymbols().stream().map(Symbol::type).allMatch(GpuTypeConversion::isConvertible)) {
                return Optional.empty();
            }

            // Join filter
            Optional<CudfAstExpression> compiledFilter;
            if (node.getFilter().isPresent()) {
                Expression filter = node.getFilter().get();
                compiledFilter = GpuJoinFilterCompiler.compile(filter);
                if (compiledFilter.isEmpty()) {
                    log.debug("Could not compile join filter for GPU execution for join type %s: %s", node.getType(), filter);
                    return Optional.empty();
                }
            }
            else {
                compiledFilter = Optional.empty();
            }

            // Force single build driver: GPU handles build-side parallelism internally.
            buildContext.setDriverInstanceCount(1);
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(node.getRightOutputSymbols(), buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));

            boolean partitioned = !isBuildSideReplicated(node);
            Optional<LocalDynamicFilterConsumer> localDynamicFilter = createDynamicFilter(buildSource, node, context, localDynamicFilters, partitioned);
            Optional<GpuDynamicFilterCollector> gpuDynamicFilter = localDynamicFilter
                    .map(filter -> buildGpuDynamicFilterCollector(filter, buildSource, partitioned));
            localDynamicFilter.ifPresent(filter -> filter.setPartitionCount(1));

            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            Optional<AstExpression> filter = compiledFilter.map(ast -> ast.toCudfAst(probeSource.getLayout(), buildLayout));

            GpuJoinBridgeManager bridgeManager = new GpuJoinBridgeManager();

            PhysicalOperation joinBuild = addGpuOperation(
                    new GpuJoinBuild.Factory(
                            bridgeManager,
                            Ints.toArray(buildChannels),
                            Ints.toArray(buildOutputChannels),
                            filter,
                            gpuDynamicFilter),
                    ImmutableList.of(),
                    buildSource,
                    ImmutableMap.of(),
                    buildContext,
                    node.getId());

            // For the last operator, Driver does not call getOutput(), only addInput() (guarded by needsInput()) and finish() (when input exhausted).
            // This means that the sink operator can never declare "I temporarily do not want more input", which is incompatible with GPU's operations
            // contract such as BufferPages. We're a dummy operator so that Driver calls getOutput() allowing the build side to do its work.
            joinBuild = new PhysicalOperation(new SentinelSinkOperator.Factory(buildContext.getNextOperatorId(), node.getId()), ImmutableMap.of(), joinBuild);

            context.addDriverFactory(false, joinBuild, buildContext);

            List<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            List<Type> joinOutputTypes = ImmutableList.<Type>builder()
                    .addAll(probeOutputChannels.stream().map(probeSource.getTypes()::get).collect(toImmutableList()))
                    .addAll(buildOutputTypes)
                    .build();

            // Probe pipeline
            GpuLookupJoin.Factory probeFactory = new GpuLookupJoin.Factory(
                    bridgeManager,
                    Ints.toArray(probeJoinChannels),
                    Ints.toArray(probeOutputChannels),
                    joinType,
                    buildOutputTypes,
                    filter.isPresent());

            return Optional.of(addGpuOperation(
                    probeFactory,
                    joinOutputTypes,
                    probeSource,
                    makeLayout(node),
                    context,
                    node.getId()));
        }

        private GpuDynamicFilterCollector buildGpuDynamicFilterCollector(
                LocalDynamicFilterConsumer consumer,
                PhysicalOperation buildSource,
                boolean partitioned)
        {
            List<GpuDynamicFilterCollector.Channel> channels = consumer.getBuildChannels().entrySet().stream()
                    .map(entry -> {
                        Type type = buildSource.getTypes().get(entry.getValue());
                        Optional<GpuTypeConversion.GpuTypeMapping> mapping = GpuTypeConversion.toGpuMapping(type);
                        return new GpuDynamicFilterCollector.Channel(
                                entry.getKey(),
                                entry.getValue(),
                                type,
                                mapping.flatMap(GpuTypeConversion.GpuTypeMapping::fromScalar));
                    })
                    .collect(toImmutableList());
            int maxDistinctValues = multipleIf(getDynamicFilteringMaxDistinctValuesPerDriver(partitioned), getTaskConcurrency(session), true);
            return new GpuDynamicFilterCollector(consumer, channels, maxDistinctValues);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<Symbol, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupBySymbols,
                Step step,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                boolean isStreamable,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize)
        {
            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregatorFactory> aggregatorFactories = new ArrayList<>();
            for (Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();

                aggregatorFactories.add(buildAggregatorFactory(source, aggregation, step));
                aggregationOutputSymbols.add(symbol);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            OptionalInt groupIdChannel = OptionalInt.empty();
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = OptionalInt.of(channel);
                }
                channel++;
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            if (isStreamable) {
                return StreamingAggregationOperator.createOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        source.getTypes(),
                        groupByTypes,
                        groupByChannels,
                        aggregatorFactories,
                        joinCompiler);
            }
            return new HashAggregationOperatorFactory(
                    context.getNextOperatorId(),
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    ImmutableList.copyOf(globalGroupingSets),
                    step,
                    hasDefaultOutput,
                    aggregatorFactories,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialAggregationMemorySize,
                    spillEnabled,
                    unspillMemoryLimit,
                    spillerFactory,
                    hashStrategyCompiler,
                    createPartialAggregationController(maxPartialAggregationMemorySize, step, session));
        }
    }

    private int getPartitionedWriterCountBasedOnMemory(Session session)
    {
        return getPartitionedWriterCountBasedOnMemory(getTaskMaxWriterCount(session), session);
    }

    private int getPartitionedWriterCountBasedOnMemory(int partitionedWriterCount, Session session)
    {
        return min(partitionedWriterCount, previousPowerOfTwo(getMaxWritersBasedOnMemory(session)));
    }

    private static Optional<PartialAggregationController> createPartialAggregationController(Optional<DataSize> maxPartialAggregationMemorySize, AggregationNode.Step step, Session session)
    {
        return maxPartialAggregationMemorySize.isPresent() && step.isOutputPartial() && isAdaptivePartialAggregationEnabled(session) ?
                Optional.of(new PartialAggregationController(
                        isUseCardinalityBasedPartialAggregationController(session),
                        maxPartialAggregationMemorySize.get(),
                        getAdaptivePartialAggregationUniqueRowsRatioThreshold(session))) :
                Optional.empty();
    }

    private PhysicalOperation addGpuOperation(
            GpuOperation.Factory gpuOperation,
            List<Type> outputTypes,
            PhysicalOperation source,
            Map<Symbol, Integer> outputLayout,
            LocalExecutionPlanContext context,
            PlanNodeId nodeId)
    {
        return addGpuOperations(ImmutableList.of(gpuOperation), outputTypes, source, outputLayout, context, nodeId);
    }

    /**
     * Append a sequence of GPU operations to {@code source} as one chained {@link GpuOperator}.
     * {@code finalOutputTypes} and {@code outputLayout} describe the operator's output after the
     * full sequence runs; intermediate stages aren't separately observable, so callers don't have
     * to thread per-stage types.
     */
    private PhysicalOperation addGpuOperations(
            List<GpuOperation.Factory> gpuOperations,
            List<Type> finalOutputTypes,
            PhysicalOperation source,
            Map<Symbol, Integer> outputLayout,
            LocalExecutionPlanContext context,
            PlanNodeId nodeId)
    {
        checkArgument(!gpuOperations.isEmpty(), "gpuOperations is empty");
        List<OperatorFactory> sourcePipeline = source.getPipelineTail();
        // Check if source is already a GPU operation - chain onto it
        if (!sourcePipeline.isEmpty() && sourcePipeline.getLast() instanceof GpuOperator.BaseFactory gpuSource) {
            List<OperatorFactory> newPipeline = ImmutableList.<OperatorFactory>builder()
                    .addAll(sourcePipeline.subList(0, sourcePipeline.size() - 1))
                    .add(gpuSource.withAdditionalOperations(ImmutableList.of(nodeId), gpuOperations, finalOutputTypes))
                    .build();
            return new PhysicalOperation(newPipeline, source.pipelineHeadAlternatives, source.chooseAlternativePlanNodeId, outputLayout);
        }
        // Source is not GPU - create new GPU operator with all operations
        return new PhysicalOperation(
                new GpuOperator.Factory(
                        context.getNextOperatorId(),
                        nodeId,
                        source.getTypes(),
                        ImmutableList.copyOf(gpuOperations),
                        finalOutputTypes),
                outputLayout,
                source);
    }

    private int getDynamicFilteringMaxDistinctValuesPerDriver(boolean partitioned)
    {
        if (partitioned) {
            return partitionedMaxDistinctValuesPerDriver;
        }
        return maxDistinctValuesPerDriver;
    }

    private int getBloomFilterMaxDistinctValuesPerDriver(boolean partitioned)
    {
        if (partitioned) {
            return partitionedBloomFilterMaxDistinctValuesPerDriver;
        }
        return bloomFilterMaxDistinctValuesPerDriver;
    }

    private DataSize getDynamicFilteringMaxSizePerDriver(boolean partitioned)
    {
        if (partitioned) {
            return partitionedMaxSizePerDriver;
        }
        return maxSizePerDriver;
    }

    private DataSize getDynamicFilteringMaxSizePerOperator(boolean partitioned)
    {
        if (partitioned) {
            return partitionedMaxSizePerOperator;
        }
        return maxSizePerOperator;
    }

    private static List<Type> getTypes(List<Expression> expressions)
    {
        return expressions.stream()
                .map(Expression::type)
                .collect(toImmutableList());
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics, tableExecuteContext) -> {
            if (target instanceof CreateTarget createTarget) {
                return metadata.finishCreateTable(session, createTarget.getHandle(), fragments, statistics);
            }
            if (target instanceof InsertTarget insertTarget) {
                return metadata.finishInsert(session, insertTarget.getHandle(), insertTarget.getSourceTableHandles(), fragments, statistics);
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewTarget refreshTarget) {
                return metadata.finishRefreshMaterializedView(
                        session,
                        refreshTarget.getTableHandle(),
                        refreshTarget.getInsertHandle(),
                        fragments,
                        statistics,
                        refreshTarget.getSourceTableHandles(),
                        refreshTarget.getSourceTableFunctions(),
                        refreshTarget.hasNonDeterministicFunctions());
            }
            if (target instanceof TableExecuteTarget tableExecuteTarget) {
                TableExecuteHandle tableExecuteHandle = tableExecuteTarget.getExecuteHandle();
                Map<String, Long> metrics = metadata.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteContext.getSplitsInfo());
                tableExecuteContext.setMetrics(metrics);
                return Optional.empty();
            }
            if (target instanceof MergeTarget mergeTarget) {
                MergeHandle mergeHandle = mergeTarget.getMergeHandle().orElseThrow(() -> new IllegalArgumentException("mergeHandle not present"));
                metadata.finishMerge(session, mergeHandle, mergeTarget.getSourceTableHandles(), fragments, statistics);
                return Optional.empty();
            }
            throw new AssertionError("Unhandled target type: " + target.getClass().getName());
        };
    }

    private static boolean shouldOutputRowCount(TableFinishNode node)
    {
        WriterTarget target = node.getTarget();
        return !(target instanceof TableExecuteTarget);
    }

    private static Function<Page, Page> enforceLoadedLayoutProcessor(List<Symbol> expectedLayout, Map<Symbol, Integer> inputLayout)
    {
        int[] channels = expectedLayout.stream()
                .peek(symbol -> checkArgument(inputLayout.containsKey(symbol), "channel not found for symbol: %s", symbol))
                .mapToInt(inputLayout::get)
                .toArray();

        if (Arrays.equals(channels, range(0, inputLayout.size()).toArray())) {
            return Function.identity();
        }

        return new PageChannelSelector(channels);
    }

    private static Page validateSpooledLayoutProcessor(Page page)
    {
        verify(page.getPositionCount() > 0, "Expected at least one position in spooled metadata block");
        verify(page.getChannelCount() == 1, "Expected a single output channel when spooling");
        verify(page.getBlock(0) instanceof RowBlock, "Expected a RowBlock for spooling metadata");
        return page;
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    private static Function<Symbol, OptionalInt> channelGetter(PhysicalOperation source)
    {
        return input -> {
            checkArgument(source.getLayout().containsKey(input));
            return OptionalInt.of(source.getLayout().get(input));
        };
    }

    private static Set<DynamicFilterId> getConsumedDynamicFilterIds(PlanNode node)
    {
        return extractExpressions(node)
                .stream()
                .flatMap(expression -> extractDynamicFilters(expression).getDynamicConjuncts().stream())
                .map(DynamicFilters.Descriptor::getId)
                .collect(toImmutableSet());
    }

    private TableHandle createCacheTableHandle()
    {
        return new TableHandle(
                createRootCatalogHandle(new CatalogName("cache"), new CatalogVersion("cache")),
                new ConnectorTableHandle() {},
                new ConnectorTransactionHandle() {});
    }

    /**
     * Encapsulates a physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private final List<OperatorFactory> pipelineTail;
        private final Map<TableHandle, List<OperatorFactory>> pipelineHeadAlternatives;
        private final Optional<PlanNodeId> chooseAlternativePlanNodeId;
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout)
        {
            this(ImmutableList.of(operatorFactory), ImmutableMap.of(), Optional.empty(), layout);
        }

        PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, PhysicalOperation source)
        {
            this(ImmutableList.<OperatorFactory>builder()
                            .addAll(source.getPipelineTail())
                            .add(operatorFactory)
                            .build(),
                    source.pipelineHeadAlternatives,
                    source.chooseAlternativePlanNodeId,
                    layout);
        }

        PhysicalOperation(
                Map<TableHandle, PhysicalOperation> pipelineHeadAlternatives,
                PlanNodeId chooseAlternativePlanNodeId,
                Map<Symbol, Integer> layout)
        {
            this(ImmutableList.of(),
                    Maps.transformValues(pipelineHeadAlternatives, PhysicalOperation::getPipelineTail),
                    Optional.of(chooseAlternativePlanNodeId),
                    layout);
        }

        private PhysicalOperation(
                List<OperatorFactory> pipelineTail,
                Map<TableHandle, List<OperatorFactory>> pipelineHeadAlternatives,
                Optional<PlanNodeId> chooseAlternativePlanNodeId,
                Map<Symbol, Integer> layout)
        {
            this.pipelineTail = ImmutableList.copyOf(requireNonNull(pipelineTail, "pipelineTail is null"));
            checkArgument(
                    chooseAlternativePlanNodeId.isEmpty() == pipelineHeadAlternatives.isEmpty(),
                    "pipelineHeadAlternatives and chooseAlternativePlanNodeId must be both provided or neither one but got: %s and %s",
                    chooseAlternativePlanNodeId,
                    pipelineHeadAlternatives);
            this.pipelineHeadAlternatives = ImmutableMap.copyOf(requireNonNull(pipelineHeadAlternatives, "pipelineHeadAlternatives is null"));
            this.chooseAlternativePlanNodeId = requireNonNull(chooseAlternativePlanNodeId, "chooseAlternativePlanNodeId is null");
            this.layout = ImmutableMap.copyOf(requireNonNull(layout, "layout is null"));
            this.types = toTypes(layout);
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s",
                    layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(symbol -> symbol.type())
                    .collect(toImmutableList());
        }

        public int symbolToChannel(Symbol input)
        {
            checkArgument(layout.containsKey(input));
            return layout.get(input);
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<Symbol, Integer> getLayout()
        {
            return layout;
        }

        private List<OperatorFactory> getOperatorFactories()
        {
            checkArgument(pipelineHeadAlternatives.isEmpty());
            return pipelineTail;
        }

        private List<OperatorFactory> getPipelineTail()
        {
            return pipelineTail;
        }
    }

    private static class SpooledPhysicalOperation
            extends PhysicalOperation
    {
        public SpooledPhysicalOperation(OutputSpoolingOperatorFactory outputSpoolingOperatorFactory, PhysicalOperation operation)
        {
            super(outputSpoolingOperatorFactory, operation.layout, operation);
        }
    }

    private static class DriverFactoryParameters
    {
        private final LocalExecutionPlanContext subContext;
        private final PhysicalOperation source;

        public DriverFactoryParameters(LocalExecutionPlanContext subContext, PhysicalOperation source)
        {
            this.subContext = subContext;
            this.source = source;
        }

        public LocalExecutionPlanContext getSubContext()
        {
            return subContext;
        }

        public PhysicalOperation getSource()
        {
            return source;
        }
    }

    private static class ValueAccessors
    {
        private final List<PhysicalValueAccessor> valueAccessors;
        private final List<MatchAggregationInstantiator> aggregations;
        private final int aggregationIndex;
        private final List<ArgumentComputationSupplier> aggregationArguments;
        private final int firstUnusedChannel;
        private final List<MatchAggregationLabelDependency> labelDependencies;

        public ValueAccessors(List<PhysicalValueAccessor> valueAccessors, List<MatchAggregationInstantiator> aggregations, int aggregationIndex, List<ArgumentComputationSupplier> aggregationArguments, int firstUnusedChannel, List<MatchAggregationLabelDependency> labelDependencies)
        {
            this.valueAccessors = valueAccessors;
            this.aggregations = aggregations;
            this.aggregationIndex = aggregationIndex;
            this.aggregationArguments = aggregationArguments;
            this.firstUnusedChannel = firstUnusedChannel;
            this.labelDependencies = labelDependencies;
        }

        public List<PhysicalValueAccessor> getValueAccessors()
        {
            return valueAccessors;
        }

        public List<MatchAggregationInstantiator> getAggregations()
        {
            return aggregations;
        }

        public int getAggregationIndex()
        {
            return aggregationIndex;
        }

        public List<ArgumentComputationSupplier> getAggregationArguments()
        {
            return aggregationArguments;
        }

        public int getFirstUnusedChannel()
        {
            return firstUnusedChannel;
        }

        public List<MatchAggregationLabelDependency> getLabelDependencies()
        {
            return labelDependencies;
        }
    }

    public static class MatchAggregationLabelDependency
    {
        private final Set<Integer> labels;
        private final boolean classifierInvolved;

        public MatchAggregationLabelDependency(Set<Integer> labels, boolean classifierInvolved)
        {
            this.labels = labels;
            this.classifierInvolved = classifierInvolved;
        }

        public Set<Integer> getLabels()
        {
            return labels;
        }

        public boolean isClassifierInvolved()
        {
            return classifierInvolved;
        }
    }

    private static class FunctionKey
    {
        private final FunctionId functionId;
        private final BoundSignature boundSignature;

        public FunctionKey(FunctionId functionId, BoundSignature boundSignature)
        {
            this.functionId = requireNonNull(functionId, "functionId is null");
            this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return functionId.equals(that.functionId) &&
                    boundSignature.equals(that.boundSignature);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionId, boundSignature);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("functionId", functionId)
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }

    @VisibleForTesting
    static boolean isGpuLocalExchangeEligible(ExchangeNode node)
    {
        if (node.getOrderingScheme().isPresent()) {
            log.debug("Could not plan local exchange for GPU execution: sort-merge ordering");
            return false;
        }
        PartitioningHandle partitioning = node.getPartitioningScheme().getPartitioning().getHandle();
        if (!partitioning.equals(SINGLE_DISTRIBUTION)
                && !partitioning.equals(FIXED_HASH_DISTRIBUTION)
                && !partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            log.debug("Could not plan local exchange for GPU execution: unsupported partitioning %s", partitioning);
            return false;
        }
        if (partitioning.getCatalogHandle().isPresent()) {
            log.debug("Could not plan local exchange for GPU execution: connector partitioning %s", partitioning);
            return false;
        }
        if (partitioning.getConnectorHandle() instanceof MergePartitioningHandle) {
            log.debug("Could not plan local exchange for GPU execution: MERGE INTO partitioning");
            return false;
        }
        List<Type> outputTypes = node.getOutputSymbols().stream()
                .map(Symbol::type)
                .collect(toImmutableList());
        if (!outputTypes.stream().allMatch(GpuTypeConversion::isConvertible)) {
            log.debug("Could not plan local exchange for GPU execution: output types not GPU-convertible: %s", outputTypes);
            return false;
        }
        List<Integer> partitionChannels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                .collect(toImmutableList());
        List<Type> partitionKeyTypes = partitionChannels.stream().map(outputTypes::get).collect(toImmutableList());
        if (!partitionKeyTypes.stream().allMatch(GpuTypeConversion::isConvertible)) {
            log.debug("Could not plan local exchange for GPU execution: partition-key types not GPU-convertible: %s", partitionKeyTypes);
            return false;
        }
        return true;
    }

    private boolean isGpuExecutionEnabled(Session session)
    {
        return nodeGpuExecutionEnabled && SystemSessionProperties.isGpuExecutionEnabled(session);
    }

    private boolean useSpillingJoinOperator(boolean spillEnabled, Session session)
    {
        return spillEnabled || isForceSpillingOperator(session);
    }
}
