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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.Rmm;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.List;

/// JMH secondary-metric profiler that reports the peak transient device memory of an iteration,
/// aggregated across iterations with [AggregationPolicy#MAX].
public class RmmPeakMemoryProfiler
        implements InternalProfiler
{
    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams)
    {
        // No-op if RMM is not yet initialized.
        Rmm.resetScopedMaximumBytesAllocated();
    }

    @Override
    public List<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result)
    {
        double peakMebibytes = Rmm.getScopedMaximumBytesAllocated() / (1024.0 * 1024.0);
        return List.of(new ScalarResult("rmm.peak", peakMebibytes, "MiB", AggregationPolicy.MAX));
    }

    @Override
    public String getDescription()
    {
        return "RMM peak transient device memory (scoped high-water) per iteration";
    }
}
