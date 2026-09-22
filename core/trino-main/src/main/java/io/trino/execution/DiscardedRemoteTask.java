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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.metadata.Split;
import io.trino.operator.TaskStats;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskStatus.failWith;
import static java.util.Objects.requireNonNull;

public final class DiscardedRemoteTask
        implements RemoteTask
{
    private final TaskInfo taskInfo;

    public DiscardedRemoteTask(TaskInfo source, ExecutionFailureInfo restartCause)
    {
        requireNonNull(source, "source is null");
        requireNonNull(restartCause, "restartCause is null");
        this.taskInfo = new TaskInfo(
                toFailedStatus(source.taskStatus(), restartCause),
                source.lastHeartbeat(),
                source.outputBuffers(),
                source.noMoreSplits(),
                withoutLiveState(source.stats()),
                source.gpuIneligibilityReasons(),
                source.estimatedMemory(),
                source.needsPlan());
    }

    @Override
    public TaskId getTaskId()
    {
        return taskInfo.taskStatus().taskId();
    }

    @Override
    public String getNodeId()
    {
        return taskInfo.taskStatus().nodeId();
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfo;
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return taskInfo.taskStatus();
    }

    @Override
    public void start() {}

    @Override
    public void addSplits(Multimap<PlanNodeId, Split> splits) {}

    @Override
    public void noMoreSplits(PlanNodeId sourceId) {}

    @Override
    public void setOutputBuffers(OutputBuffers outputBuffers) {}

    @Override
    public void setSpeculative(boolean speculative) {}

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        stateChangeListener.stateChanged(taskInfo.taskStatus());
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        stateChangeListener.stateChanged(taskInfo);
    }

    @Override
    public ListenableFuture<Void> whenSplitQueueHasSpace(long weightThreshold)
    {
        return immediateVoidFuture();
    }

    @Override
    public void cancel() {}

    @Override
    public void abort() {}

    @Override
    public PartitionedSplitsInfo getPartitionedSplitsInfo()
    {
        return PartitionedSplitsInfo.forZeroSplits();
    }

    @Override
    public void failLocallyImmediately(Throwable cause) {}

    @Override
    public void failRemotely(Throwable cause) {}

    @Override
    public void forceFinalizationUsingTaskStatus() {}

    @Override
    public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo()
    {
        return PartitionedSplitsInfo.forZeroSplits();
    }

    @Override
    public int getUnacknowledgedPartitionedSplitCount()
    {
        return 0;
    }

    @Override
    public Optional<SpoolingOutputStats.Snapshot> retrieveAndDropSpoolingOutputStats()
    {
        return Optional.empty();
    }

    private static TaskStatus toFailedStatus(TaskStatus status, ExecutionFailureInfo restartCause)
    {
        if (status.state() == FAILED) {
            // the task failed on its own before the restart, keep its original failure cause
            return status;
        }
        return failWith(status, FAILED, ImmutableList.of(restartCause));
    }

    // Zeroing the live state to not report drivers that were queued or running and memory they held.
    // Totals such as CPU time and cumulative memory are kept, as the discarded work was actually done.
    // The completed drivers stay, and they are all the drivers the discarded task has.
    private static TaskStats withoutLiveState(TaskStats stats)
    {
        return new TaskStats(
                stats.createTime(),
                stats.firstStartTime(),
                stats.lastStartTime(),
                stats.terminatingStartTime(),
                stats.lastEndTime(),
                stats.endTime(),
                stats.elapsedTime(),
                stats.queuedTime(),
                stats.completedDrivers(),
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                stats.completedDrivers(),
                stats.cumulativeUserMemory(),
                succinctBytes(0),
                stats.peakUserMemoryReservation(),
                succinctBytes(0),
                stats.spilledDataSize(),
                stats.totalScheduledTime(),
                stats.totalCpuTime(),
                stats.totalBlockedTime(),
                false,
                ImmutableSet.of(),
                stats.physicalInputDataSize(),
                stats.physicalInputPositions(),
                stats.physicalInputReadTime(),
                stats.internalNetworkInputDataSize(),
                stats.internalNetworkInputPositions(),
                stats.processedInputDataSize(),
                stats.processedInputPositions(),
                stats.inputBlockedTime(),
                stats.outputDataSize(),
                stats.outputPositions(),
                stats.outputBlockedTime(),
                stats.writerInputDataSize(),
                stats.physicalWrittenDataSize(),
                stats.maxWriterCount(),
                stats.fullGcCount(),
                stats.fullGcTime(),
                ImmutableList.of());
    }
}
