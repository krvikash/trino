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
package io.trino.plugin.iceberg.profiler.rules;

import io.trino.plugin.iceberg.IcebergInputInfo;
import io.trino.spi.profiler.ConnectorQueryProfilerFinding;
import io.trino.spi.profiler.ConnectorQueryProfilerRule;

import java.util.List;
import java.util.Optional;

public final class IcebergOverPartitionedTable
        implements ConnectorQueryProfilerRule
{
    private static final int MIN_PARTITION_FIELDS = 3;
    private static final double MIN_RECORDS_PER_FILE = 10_000;

    @Override
    public String getConnectorName()
    {
        return "iceberg";
    }

    @Override
    public List<ConnectorQueryProfilerFinding> analyze(Object connectorInfo)
    {
        if (!(connectorInfo instanceof IcebergInputInfo info)) {
            return List.of();
        }
        if (info.partitionFields().size() < MIN_PARTITION_FIELDS) {
            return List.of();
        }
        Optional<Long> totalRecords = info.totalRecords().map(Long::parseLong);
        Optional<Long> dataFiles = info.totalDataFiles().map(Long::parseLong);
        if (totalRecords.isEmpty() || dataFiles.isEmpty() || dataFiles.get() == 0) {
            return List.of();
        }
        double recordsPerFile = (double) totalRecords.get() / dataFiles.get();
        if (recordsPerFile >= MIN_RECORDS_PER_FILE) {
            return List.of();
        }
        return List.of(new ConnectorQueryProfilerFinding(
                "Table appears over-partitioned: %s records across %s data files (records/file=%.2f, threshold=%.0f)".formatted(
                        totalRecords.get(), dataFiles.get(), recordsPerFile, MIN_RECORDS_PER_FILE)));
    }
}
