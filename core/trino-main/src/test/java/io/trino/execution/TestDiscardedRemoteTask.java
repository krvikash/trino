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
import com.google.common.collect.ImmutableListMultimap;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.operator.TaskStats;
import io.trino.spi.QueryId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.util.Failures;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.execution.TaskState.FAILED;
import static io.trino.execution.TaskState.FINISHED;
import static io.trino.execution.TaskStatus.failWith;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static io.trino.operator.TestTaskStats.EXPECTED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscardedRemoteTask
{
    private static final TaskId TASK_ID = new TaskId(new StageId(new QueryId("query"), 3), 7, 0);
    private static final URI TASK_URI = URI.create("http://node/task");
    private static final String NODE_ID = "node-1";

    @Test
    public void testFinishedSourceReassignedToFailed()
    {
        TaskInfo source = withState(initialTask(), FINISHED);
        ExecutionFailureInfo cause = Failures.toFailure(new RuntimeException("restart"));

        DiscardedRemoteTask discarded = new DiscardedRemoteTask(source, cause);

        assertThat(discarded.getTaskStatus().state()).isEqualTo(FAILED);
        assertThat(discarded.getTaskStatus().failures()).hasSize(1);
        assertThat(discarded.getTaskStatus().failures().get(0).message()).isEqualTo("restart");
    }

    @Test
    public void testCanceledSourceReassignedToFailed()
    {
        DiscardedRemoteTask discarded = new DiscardedRemoteTask(
                withState(initialTask(), TaskState.CANCELED),
                Failures.toFailure(new RuntimeException("restart")));

        assertThat(discarded.getTaskStatus().state()).isEqualTo(FAILED);
    }

    @Test
    public void testAbortedSourceReassignedToFailed()
    {
        DiscardedRemoteTask discarded = new DiscardedRemoteTask(
                withState(initialTask(), TaskState.ABORTED),
                Failures.toFailure(new RuntimeException("restart")));

        assertThat(discarded.getTaskStatus().state()).isEqualTo(FAILED);
    }

    @Test
    public void testAlreadyFailedSourceKeepsOriginalCause()
    {
        TaskInfo source = failed(initialTask(), Failures.toFailure(new RuntimeException("original")));
        ExecutionFailureInfo restartCause = Failures.toFailure(new RuntimeException("restart"));

        DiscardedRemoteTask discarded = new DiscardedRemoteTask(source, restartCause);

        assertThat(discarded.getTaskStatus().state()).isEqualTo(FAILED);
        List<ExecutionFailureInfo> failures = discarded.getTaskStatus().failures();
        assertThat(failures).hasSize(1);
        assertThat(failures.get(0).message()).isEqualTo("original");
    }

    @Test
    public void testLiveStateOfSourceIsDropped()
    {
        TaskInfo source = TaskInfo.createInitialTask(TASK_ID, TASK_URI, NODE_ID, false, Optional.empty(), EXPECTED);

        TaskStats stats = new DiscardedRemoteTask(source, Failures.toFailure(new RuntimeException("restart"))).getTaskInfo().stats();

        // only the completed drivers remain; the others are gone
        assertThat(stats.totalDrivers()).isEqualTo(EXPECTED.completedDrivers());
        assertThat(stats.queuedDrivers()).isZero();
        assertThat(stats.queuedPartitionedDrivers()).isZero();
        assertThat(stats.queuedPartitionedSplitsWeight()).isZero();
        assertThat(stats.runningDrivers()).isZero();
        assertThat(stats.runningPartitionedDrivers()).isZero();
        assertThat(stats.runningPartitionedSplitsWeight()).isZero();
        assertThat(stats.blockedDrivers()).isZero();
        assertThat(stats.completedDrivers()).isEqualTo(EXPECTED.completedDrivers());
        assertThat(stats.pipelines()).isEmpty();

        // the memory is released
        assertThat(stats.userMemoryReservation().toBytes()).isZero();
        assertThat(stats.revocableMemoryReservation().toBytes()).isZero();
        assertThat(stats.fullyBlocked()).isFalse();
        assertThat(stats.blockedReasons()).isEmpty();

        // the work was done
        assertThat(stats.totalCpuTime()).isEqualTo(EXPECTED.totalCpuTime());
        assertThat(stats.totalScheduledTime()).isEqualTo(EXPECTED.totalScheduledTime());
        assertThat(stats.cumulativeUserMemory()).isEqualTo(EXPECTED.cumulativeUserMemory());
        assertThat(stats.peakUserMemoryReservation()).isEqualTo(EXPECTED.peakUserMemoryReservation());
        assertThat(stats.physicalInputDataSize()).isEqualTo(EXPECTED.physicalInputDataSize());
    }

    @Test
    public void testIdentityGettersReflectSource()
    {
        TaskInfo source = withState(initialTask(), FINISHED);
        DiscardedRemoteTask discarded = new DiscardedRemoteTask(source, Failures.toFailure(new RuntimeException("x")));

        assertThat(discarded.getTaskId()).isEqualTo(TASK_ID);
        assertThat(discarded.getNodeId()).isEqualTo(NODE_ID);
        assertThat(discarded.getTaskInfo().taskStatus()).isSameAs(discarded.getTaskStatus());
    }

    @Test
    public void testMutatorsAreNoop()
    {
        DiscardedRemoteTask discarded = new DiscardedRemoteTask(
                withState(initialTask(), FINISHED),
                Failures.toFailure(new RuntimeException("x")));
        TaskStatus before = discarded.getTaskStatus();

        discarded.start();
        discarded.addSplits(ImmutableListMultimap.of());
        discarded.noMoreSplits(new PlanNodeId("plan"));
        discarded.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY));
        discarded.setSpeculative(true);
        discarded.cancel();
        discarded.abort();
        discarded.failLocallyImmediately(new RuntimeException("ignored"));
        discarded.failRemotely(new RuntimeException("ignored"));
        discarded.forceFinalizationUsingTaskStatus();

        // Reference equality: task snapshot never mutates
        assertThat(discarded.getTaskStatus()).isSameAs(before);
        assertThat(discarded.getPartitionedSplitsInfo().getCount()).isEqualTo(0);
        assertThat(discarded.getQueuedPartitionedSplitsInfo().getCount()).isEqualTo(0);
        assertThat(discarded.getUnacknowledgedPartitionedSplitCount()).isEqualTo(0);
        assertThat(discarded.retrieveAndDropSpoolingOutputStats()).isEmpty();
        assertThat(discarded.whenSplitQueueHasSpace(1L).isDone()).isTrue();
    }

    @Test
    public void testListenersFireOnceWithTerminalState()
    {
        DiscardedRemoteTask discarded = new DiscardedRemoteTask(
                withState(initialTask(), FINISHED),
                Failures.toFailure(new RuntimeException("x")));

        AtomicReference<TaskStatus> statusRef = new AtomicReference<>();
        AtomicReference<TaskInfo> infoRef = new AtomicReference<>();
        discarded.addStateChangeListener(statusRef::set);
        discarded.addFinalTaskInfoListener(infoRef::set);

        assertThat(statusRef.get()).isNotNull();
        assertThat(statusRef.get().state()).isEqualTo(FAILED);
        assertThat(infoRef.get()).isNotNull();
        assertThat(infoRef.get().taskStatus().state()).isEqualTo(FAILED);
    }

    private static TaskInfo initialTask()
    {
        return TaskInfo.createInitialTask(
                TASK_ID,
                TASK_URI,
                NODE_ID,
                false,
                Optional.empty(),
                new TaskStats(Instant.now(), null));
    }

    private static TaskInfo withState(TaskInfo taskInfo, TaskState state)
    {
        return taskInfo.withTaskStatus(failWith(taskInfo.taskStatus(), state, ImmutableList.of()));
    }

    private static TaskInfo failed(TaskInfo taskInfo, ExecutionFailureInfo cause)
    {
        return taskInfo.withTaskStatus(failWith(taskInfo.taskStatus(), FAILED, ImmutableList.of(cause)));
    }
}
