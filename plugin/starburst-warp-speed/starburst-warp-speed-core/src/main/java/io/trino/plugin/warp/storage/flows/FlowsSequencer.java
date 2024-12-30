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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class FlowsSequencer
{
    public static final String STATS_GROUP_NAME = "flowsequencer";
    public static final long INVALID_FLOW_ID = Long.MIN_VALUE;

    private static final AtomicLong flowIdGen = new AtomicLong(Long.MIN_VALUE);

    private final FlowPriorityQueue flowPriorityQueue;

    @Inject
    public FlowsSequencer()
    {
        flowPriorityQueue = new FlowPriorityQueue();
    }

    public CompletableFuture<Long> tryRunningFlow(FlowType flowType, Optional<String> additionalInfo)
    {
        return flowPriorityQueue.addFlow(flowType, flowIdGen.incrementAndGet(), additionalInfo);
    }

    public boolean flowFinished(FlowType flowType, long flowId, boolean force)
    {
        if (flowId != INVALID_FLOW_ID) {
            flowPriorityQueue.removeFlow(flowType, flowId, force);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    public Map<String, ?> getRunningFlows()
    {
        return flowPriorityQueue.getRunningFlows();
    }

    @VisibleForTesting
    public Map<String, ?> getPendingFlows()
    {
        return flowPriorityQueue.getPendingFlows();
    }
}
