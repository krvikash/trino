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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.gpu.GpuOperator.FUSED_PLAN_NODE_METRIC_PREFIX;
import static io.trino.operator.gpu.GpuTableScan.CPU_FALLBACK_SPLITS_METRIC;
import static io.trino.sql.planner.planprinter.PlanNodeGpuStatus.eligible;
import static io.trino.sql.planner.planprinter.PlanNodeGpuStatus.notEligible;
import static io.trino.sql.planner.planprinter.PlanNodeGpuStatus.partiallyEligible;

public final class PlanNodeGpuStatusSummarizer
{
    private PlanNodeGpuStatusSummarizer() {}

    public static Map<PlanNodeId, PlanNodeGpuStatus> aggregateGpuStatuses(List<StageInfo> stageInfos)
    {
        Map<PlanNodeId, PlanNodeGpuStatus> gpuStatuses = new HashMap<>();

        for (StageInfo stageInfo : stageInfos) {
            for (TaskInfo taskInfo : stageInfo.tasks()) {
                Map<PlanNodeId, PlanNodeGpuStatus> taskGpuStatuses = getGpuStatuses(taskInfo);
                for (Map.Entry<PlanNodeId, PlanNodeGpuStatus> status : taskGpuStatuses.entrySet()) {
                    mergeAcrossTasks(gpuStatuses, status.getKey(), status.getValue());
                }
            }
        }

        return ImmutableMap.copyOf(gpuStatuses);
    }

    private static Map<PlanNodeId, PlanNodeGpuStatus> getGpuStatuses(TaskInfo taskInfo)
    {
        Map<PlanNodeId, PlanNodeGpuStatus> gpuStatuses = new HashMap<>();

        Map<PlanNodeId, String> gpuIneligibilityReasons = taskInfo.gpuIneligibilityReasons();
        gpuIneligibilityReasons
                .forEach((key, value) -> gpuStatuses.put(key, notEligible(value)));

        for (PipelineStats pipelineStats : taskInfo.stats().pipelines()) {
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();
                if (operatorStats.getOperatorType().equals("GpuOperator")) {
                    ensureNotMarkedAsGpuIneligible(gpuIneligibilityReasons, planNodeId);
                    operatorStats.getMetrics().getMetrics().keySet().stream()
                            .filter(metric -> metric.startsWith(FUSED_PLAN_NODE_METRIC_PREFIX))
                            .map(metric -> metric.substring(FUSED_PLAN_NODE_METRIC_PREFIX.length()))
                            .map(PlanNodeId::new)
                            .forEach(nodeId -> {
                                ensureNotMarkedAsGpuIneligible(gpuIneligibilityReasons, nodeId);
                                mergeWithinTask(gpuStatuses, nodeId, eligible());
                            });
                    Metric<?> metric = operatorStats.getMetrics().getMetrics().get(CPU_FALLBACK_SPLITS_METRIC);
                    if (metric != null) {
                        long cpuSplits = ((Count<?>) metric).getTotal();
                        long gpuSplits = operatorStats.getTotalDrivers() - cpuSplits;
                        mergeWithinTask(gpuStatuses, planNodeId, partiallyEligible(cpuSplits, gpuSplits));
                    }
                    else {
                        mergeWithinTask(gpuStatuses, planNodeId, eligible());
                    }
                }
            }
        }

        return gpuStatuses;
    }

    private static void ensureNotMarkedAsGpuIneligible(Map<PlanNodeId, String> gpuIneligibilityReasons, PlanNodeId planNodeId)
    {
        checkState(!gpuIneligibilityReasons.containsKey(planNodeId), "plan node %s is marked as ineligible for GPU execution but has a GpuOperator associated with it", planNodeId);
    }

    private static void mergeWithinTask(Map<PlanNodeId, PlanNodeGpuStatus> gpuStatuses, PlanNodeId planNodeId, PlanNodeGpuStatus status)
    {
        gpuStatuses.merge(planNodeId, status, PlanNodeGpuStatus::mergeWithinTask);
    }

    private static void mergeAcrossTasks(Map<PlanNodeId, PlanNodeGpuStatus> gpuStatuses, PlanNodeId planNodeId, PlanNodeGpuStatus status)
    {
        gpuStatuses.merge(planNodeId, status, PlanNodeGpuStatus::mergeAcrossTasks);
    }
}
