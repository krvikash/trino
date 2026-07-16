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
package io.trino.tests.benchmark;

import io.airlift.units.DataSize;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Per-benchmark customization for the {@link BenchmarkRunner} harness — describes which
 * queries to run and how to set up the data.
 */
public interface Workload
{
    /**
     * Short identifier used as the per-workload subdirectory name under the output dir.
     */
    String name();

    /**
     * Queries to run when no {@code --query} flag is passed.
     */
    List<Integer> defaultQueries();

    /**
     * Load the SQL text for a query (typically from a classpath resource).
     */
    String readQuery(int queryNumber);

    /**
     * Default value for the {@code --data} flag when not passed on the command line.
     */
    String defaultDataLocation();

    /**
     * JVM heap size for the forked benchmark process.
     */
    DataSize jvmHeapSize();

    /**
     * Build a query runner with all benchmark tables registered, applying mode-specific extras.
     */
    DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> fsCacheDirectory)
            throws Exception;

    /**
     * Optional sanity check on the data directory before the runner starts.
     */
    default void validateDataLocation(String dataLocation) {}

    /**
     * async-profiler sampling interval. Below ~5 ms on macOS, SIGPROF delivery collapses
     * mid-run and later queries record empty profiles — workloads with longer queries should
     * pick a coarser value to extend the profiler's effective lifetime across the run.
     */
    default String profileInterval()
    {
        return "5ms";
    }

    /**
     * Sanity check on the running query runner. Throw to fail the run before queries execute.
     */
    void verifyDataset(DistributedQueryRunner runner);

    /**
     * Fully-qualified names of tables the workload's queries read. The harness aborts if any
     * table is missing statistics — stat-less plans aren't representative of real behavior.
     */
    List<String> tablesForStats();

    /**
     * Classpath resource path for the expected NDJSON result. {@code run} reads from the
     * classpath (works in both source-tree and jar layouts); {@code record} writes back under
     * {@code testing/trino-benchmark-queries/src/main/resources/} (requires a source tree).
     */
    String expectedResultResource(int queryNumber);

    /**
     * One-shot data generator invoked by the {@code generate} subcommand.
     */
    default void generateData(Path targetLocation)
            throws Exception
    {
        throw new UnsupportedOperationException("Data generation not supported for " + name());
    }
}
