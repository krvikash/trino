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
package io.trino.plugin.iceberg;

import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.metrics.Metric;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergParquetRemoteSplitsGenerationConnectorTest
        extends BaseIcebergParquetConnectorTest
{
    public BaseIcebergParquetRemoteSplitsGenerationConnectorTest(int formatVersion)
    {
        super(formatVersion);
    }

    @Override
    protected IcebergQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                .addIcebergProperty("iceberg.remote-splits-generation.enabled", "true")
                .addIcebergProperty("iceberg.remote-splits-generation.manifests-per-thread", "0");
    }

    @Test
    void testRemoteSplitsGenerationEnabled()
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT name FROM region");

        Map<String, Metric<?>> metrics = getOperatorStats(result.queryId()).getConnectorMetrics().getMetrics();
        assertThat(((LongCount) metrics.get("remoteSplitsSource.taskCreateAttempts")).getTotal()).isGreaterThanOrEqualTo(1);
        assertThat(((LongCount) metrics.get("remoteSplitsSource.batchesFetched")).getTotal()).isGreaterThanOrEqualTo(1);
    }
}
