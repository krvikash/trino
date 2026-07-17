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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public record PlanNodeGpuStatus(long cpuSplitCount, long gpuSplitCount, long cpuTaskCount, long gpuTaskCount, Set<String> reasons)
{
    public static PlanNodeGpuStatus notEligible(String reason)
    {
        return new PlanNodeGpuStatus(0, 0, 1, 0, ImmutableSet.of(reason));
    }

    public static PlanNodeGpuStatus eligible()
    {
        return new PlanNodeGpuStatus(0, 0, 0, 1, ImmutableSet.of());
    }

    public static PlanNodeGpuStatus partiallyEligible(long cpuSplits, long gpuSplits)
    {
        return new PlanNodeGpuStatus(cpuSplits, gpuSplits, 0, 1, ImmutableSet.of());
    }

    public PlanNodeGpuStatus
    {
        checkArgument(cpuSplitCount >= 0, "cpuSplitCount must not be negative");
        checkArgument(gpuSplitCount >= 0, "gpuSplitCount must not be negative");
        checkArgument(cpuTaskCount >= 0, "cpuTaskCount must not be negative");
        checkArgument(gpuTaskCount >= 0, "gpuTaskCount must not be negative");
        checkArgument(reasons.isEmpty() == (cpuTaskCount == 0), "reasons must be present if and only if there are CPU tasks, got reasons: %s, cpuTaskCount: %s", reasons, cpuTaskCount);
        reasons = ImmutableSet.copyOf(reasons);
    }

    public PlanNodeGpuStatus mergeWithinTask(PlanNodeGpuStatus other)
    {
        checkState(cpuTaskCount == 0 && reasons.isEmpty(), "cannot merge ineligible statuses");
        checkArgument(other.cpuTaskCount == 0 && other.reasons.isEmpty(), "cannot merge ineligible statuses");
        checkArgument(other.gpuTaskCount == gpuTaskCount, "gpuTaskCount mismatch within task: %s vs %s", other.gpuTaskCount, gpuTaskCount);
        return new PlanNodeGpuStatus(
                other.cpuSplitCount + cpuSplitCount,
                other.gpuSplitCount + gpuSplitCount,
                0,
                gpuTaskCount,
                ImmutableSet.of());
    }

    public PlanNodeGpuStatus mergeAcrossTasks(PlanNodeGpuStatus other)
    {
        return new PlanNodeGpuStatus(
                other.cpuSplitCount + cpuSplitCount,
                other.gpuSplitCount + gpuSplitCount,
                other.cpuTaskCount + cpuTaskCount,
                other.gpuTaskCount + gpuTaskCount,
                ImmutableSet.<String>builder().addAll(other.reasons).addAll(reasons).build());
    }

    public boolean isNotEligible()
    {
        return gpuTaskCount == 0;
    }

    public boolean isEligible()
    {
        return !isNotEligible();
    }
}
