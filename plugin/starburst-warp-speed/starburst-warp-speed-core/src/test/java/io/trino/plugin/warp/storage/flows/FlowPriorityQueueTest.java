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
package io.trino.plugin.warp.storage.flows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class FlowPriorityQueueTest
{
    private FlowPriorityQueue flowPriorityQueue;

    @BeforeEach
    public void before()
    {
        flowPriorityQueue = new FlowPriorityQueue();
    }

    @Test
    public void testFirstAddShouldReturnFinishedFuture()
    {
        CompletableFuture<Long> firstJob = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        assertThat(firstJob.isDone()).isTrue();
    }

    @Test
    public void testDuplicateKeyShouldThrow()
    {
        CompletableFuture<Long> booleanCompletableFuture1 = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        Assertions.assertThrows(RuntimeException.class, () -> {
            CompletableFuture<Long> unused = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
            finalizeJobs(unused);
        });
        finalizeJobs(booleanCompletableFuture1);
    }

    @Test
    public void testSecondSameTypeShouldReturnFinishedFuture()
    {
        IntStream.range(0, 10).forEach(i -> {
            CompletableFuture<Long> futureJob = flowPriorityQueue.addFlow(FlowType.WARMUP, i, Optional.empty());
            assertThat(futureJob.isDone()).isTrue();
        });
    }

    @Test
    public void testSecondDifferentTypeShouldReturnNonFinishedFuture()
    {
        CompletableFuture<Long> firstJob = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        CompletableFuture<Long> secondJob = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 2, Optional.empty());
        assertThat(secondJob.isDone()).isFalse();
        finalizeJobs(firstJob, secondJob);
    }

    @Test
    public void testFinishJobShouldCompleteOther()
    {
        CompletableFuture<Long> firstJob = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        CompletableFuture<Long> secondJob = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 2, Optional.empty());
        assertThat(secondJob.isDone()).isFalse();
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 1, false);
        assertThat(secondJob.isDone()).isTrue();
        finalizeJobs(firstJob, secondJob);
    }

    @Test
    public void testFinishWithoutWaitingJobs()
    {
        CompletableFuture<Long> firstJob = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 1, false);
        CompletableFuture<Long> anotherJob = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        assertThat(anotherJob.isDone()).isTrue();
        finalizeJobs(firstJob, anotherJob);
    }

    @Test
    public void testDuplicateFinish()
    {
        CompletableFuture<Long> job = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 1, false);
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 1, false);
        finalizeJobs(job);
    }

    @Test
    public void testLowPriorityAfterDuplicateFinishShouldRun()
    {
        CompletableFuture<Long> job1 = flowPriorityQueue.addFlow(FlowType.WARMUP, 1, Optional.empty());
        CompletableFuture<Long> job2 = flowPriorityQueue.addFlow(FlowType.WARMUP, 2, Optional.empty());
        CompletableFuture<Long> job3 = flowPriorityQueue.addFlow(FlowType.WARMUP, 3, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 3, false);
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 2, false);
        CompletableFuture<Long> job4 = flowPriorityQueue.addFlow(FlowType.WARMUP, 4, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 4, false);
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 4, false);
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 1, false);
        CompletableFuture<Long> rsyncJob = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 4, Optional.empty());
        assertThat(rsyncJob.isDone()).isTrue();
        finalizeJobs(job1, job2, job3, job4, rsyncJob);
    }

    @Test
    public void testHigherPriorityShouldWaitToFinishAllExecutingLowPriority()
    {
        CompletableFuture<Long> job1 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 1, Optional.empty());
        assertThat(job1.isDone()).isTrue();
        CompletableFuture<Long> job2 = flowPriorityQueue.addFlow(FlowType.WARMUP, 2, Optional.empty());
        assertThat(job2.isDone()).isFalse();
        CompletableFuture<Long> job3 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 3, Optional.empty());
        assertThat(job3.isDone()).isFalse();

        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 1, false);
        assertThat(job2.isDone()).isTrue();
        assertThat(job3.isDone()).isFalse();
        flowPriorityQueue.removeFlow(FlowType.WARMUP, 2, false);
        assertThat(job3.isDone()).isTrue();
        finalizeJobs(job1, job2, job3);
    }

    @Test
    public void testHigherPriorityShouldWaitToFinishAllExecutingLowPriority2()
    {
        CompletableFuture<Long> job1 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 25, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 25, false);
        CompletableFuture<Long> job2 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 26, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 26, false);
        CompletableFuture<Long> job3 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 27, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 27, false);
        CompletableFuture<Long> job4 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 28, Optional.empty());
        CompletableFuture<Long> job5 = flowPriorityQueue.addFlow(FlowType.WARMUP, 29, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 28, false);
        CompletableFuture<Long> job6 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 30, Optional.empty());
        CompletableFuture<Long> job31 = flowPriorityQueue.addFlow(FlowType.WARMUP, 31, Optional.empty());
        assertThat(job31.isDone()).isTrue();
        finalizeJobs(job1, job2, job3, job4, job5, job6, job31);
    }

    @Test
    public void testMultiWaitingShouldCompleteAll()
    {
        CompletableFuture<Long> job1 = flowPriorityQueue.addFlow(FlowType.WARMUP_DEMOTER, 1, Optional.empty());
        CompletableFuture<Long> job2 = flowPriorityQueue.addFlow(FlowType.WARMUP, 2, Optional.empty());
        CompletableFuture<Long> job3 = flowPriorityQueue.addFlow(FlowType.WARMUP, 3, Optional.empty());
        flowPriorityQueue.removeFlow(FlowType.WARMUP_DEMOTER, 1, false);
        assertThat(job2.isDone()).isTrue();
        assertThat(job3.isDone()).isTrue();
        finalizeJobs(job1, job2, job2);
    }

    private void finalizeJobs(CompletableFuture<?>... jobs)
    {
        Arrays.stream(jobs).forEach(job -> {
            try {
                job.cancel(false);
            }
            catch (Throwable e) {
                // ignore
            }
        });
    }
}
