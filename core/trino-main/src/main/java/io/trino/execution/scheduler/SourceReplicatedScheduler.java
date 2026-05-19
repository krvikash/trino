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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.execution.RemoteTask;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.operator.ExchangeOperator;
import io.trino.split.EmptySplit;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static java.util.Objects.requireNonNull;

public class SourceReplicatedScheduler
        implements SourceScheduler
{
    private static final Logger log = Logger.get(SourceReplicatedScheduler.class);

    private enum State
    {
        /**
         * No splits have been added to pendingSplits set.
         */
        INITIALIZED,

        /**
         * At least one split has been added to pendingSplits set.
         */
        SPLITS_ADDED,

        /**
         * All splits have been provided to caller of this scheduler.
         * Cleanup operations are done
         */
        FINISHED,
    }

    private final StageExecution stageExecution;
    private final SplitSource splitSource;
    private final int splitBatchSize;
    private final PlanNodeId replicatedNode;
    private final Map<InternalNode, RemoteTask> scheduledTasks;

    private ListenableFuture<SplitBatch> nextSplitBatchFuture;
    private State state = State.INITIALIZED;

    public SourceReplicatedScheduler(
            StageExecution stageExecution,
            PlanNodeId replicatedNode,
            SplitSource splitSource,
            int splitBatchSize,
            Map<InternalNode, RemoteTask> scheduledTasks)
    {
        // we do not need dynamic filter support here as SourceReplicatedScheduler is only used with spooling exchanges and not connectors.
        // This is enforced in PipelinedQueryScheduler when replicatedRemoteSourceNodeIds are computed
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;
        this.replicatedNode = requireNonNull(replicatedNode, "partitionedNode is null");
        this.scheduledTasks = requireNonNull(scheduledTasks, "scheduledTasks is null");

        checkArgument(splitSource.getTableExecuteSplitsInfo().isEmpty(), "table execute split info not supported");
    }

    @Override
    public PlanNodeId getPlanNodeId()
    {
        return replicatedNode;
    }

    @Override
    public synchronized void start() {}

    @Override
    public synchronized ScheduleResult schedule()
    {
        if (state == State.FINISHED) {
            return new ScheduleResult(true, ImmutableSet.of(), 0);
        }

        Optional<ListenableFuture<Void>> blockedFuture = Optional.empty();

        ImmutableListMultimap.Builder<PlanNodeId, Split> splitBuilder = ImmutableListMultimap.builder();

        // try to get the next batch
        if (nextSplitBatchFuture == null) {
            nextSplitBatchFuture = splitSource.getNextBatch(splitBatchSize);

            long start = System.nanoTime();
            addSuccessCallback(nextSplitBatchFuture, () -> stageExecution.recordSplitSourceMetrics(replicatedNode, splitSource.getMetrics(), start));
        }

        if (nextSplitBatchFuture.isDone()) {
            SplitBatch nextSplits = getFutureValue(nextSplitBatchFuture);
            nextSplitBatchFuture = null;
            splitBuilder.putAll(replicatedNode, nextSplits.getSplits());
            if (nextSplits.isLastBatch()) {
                if (state == State.INITIALIZED && nextSplits.getSplits().isEmpty() && !splitSource.getCatalogHandle().equals(ExchangeOperator.REMOTE_CATALOG_HANDLE)) {
                    // Add an empty split in case no splits have been produced for the source.
                    // For source operators, they never take input, but they may produce output.
                    // This is well handled by the execution engine.
                    // However, there are certain non-source operators that may produce output without any input,
                    // for example, 1) an AggregationOperator, 2) a HashAggregationOperator where one of the grouping sets is ().
                    // Scheduling an empty split kicks off necessary driver instantiation to make this work.
                    // This logic is not needed for spooling exchange split source as split containing ExchangeSourceOutputSelector will be always emitted
                    splitBuilder.put(replicatedNode, new Split(splitSource.getCatalogHandle(), new EmptySplit(splitSource.getCatalogHandle())));
                }
                log.debug("stage id: %s, node: %s; transitioning to FINISHED", stageExecution.getStageId(), replicatedNode);
                state = State.FINISHED;
                splitSource.close();
            }
        }
        else {
            blockedFuture = Optional.of(asVoid(nextSplitBatchFuture));
            log.debug("stage id: %s, node: %s; blocked on next split batch", stageExecution.getStageId(), replicatedNode);
        }

        ListMultimap<PlanNodeId, Split> splits = splitBuilder.build();

        if (!splits.isEmpty() && state == State.INITIALIZED) {
            log.debug("stage id: %s, node: %s; transitioning to SPLITS_ADDED", stageExecution.getStageId(), replicatedNode);
            state = State.SPLITS_ADDED;
        }

        int scheduledSplitsCount = 0;
        for (RemoteTask task : scheduledTasks.values()) {
            task.addSplits(splits);
            scheduledSplitsCount += splits.size();
        }

        if (state == State.FINISHED) {
            verify(blockedFuture.isEmpty());
            log.debug("stage id: %s, node: %s; assigned %s splits (not blocked)", stageExecution.getStageId(), replicatedNode, scheduledSplitsCount);
            return new ScheduleResult(true, ImmutableList.of(), scheduledSplitsCount, splits);
        }

        if (blockedFuture.isEmpty()) {
            log.debug("stage id: %s, node: %s; assigned %s splits (not blocked)", stageExecution.getStageId(), replicatedNode, scheduledSplitsCount);
            return new ScheduleResult(false, ImmutableList.of(), scheduledSplitsCount);
        }

        log.debug("stage id: %s, node: %s; assigned %s splits (blocked reason WAITING_FOR_SOURCE)", stageExecution.getStageId(), replicatedNode, scheduledSplitsCount);
        return new ScheduleResult(
                false,
                ImmutableList.of(),
                nonCancellationPropagating(blockedFuture.get()),
                WAITING_FOR_SOURCE,
                scheduledSplitsCount,
                splits);
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, _ -> null, directExecutor());
    }

    @Override
    public void close()
    {
        splitSource.close();
    }
}
