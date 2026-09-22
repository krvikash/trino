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

import static io.trino.spi.profiler.Severity.CRITICAL;

public final class IcebergTooManyDeleteFiles
        implements ConnectorQueryProfilerRule
{
    private static final int MAX_TOTAL_DELETE_FILES = 2000;
    private static final double DELETE_TO_TOTAL_RATIO = 0.2;

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
        Optional<Long> deleteFiles = info.totalDeleteFiles().map(Long::parseLong);
        Optional<Long> dataFiles = info.totalDataFiles().map(Long::parseLong);
        if (deleteFiles.isEmpty() || dataFiles.isEmpty() || dataFiles.get() == 0) {
            return List.of();
        }
        if (deleteFiles.get() <= MAX_TOTAL_DELETE_FILES
                || (double) deleteFiles.get() / dataFiles.get() <= DELETE_TO_TOTAL_RATIO) {
            return List.of();
        }
        return List.of(new ConnectorQueryProfilerFinding(
                "Too many delete files (amount %s, delete to all ratio: %.2f)".formatted(
                        deleteFiles.get(), (double) deleteFiles.get() / dataFiles.get()),
                CRITICAL,
                Optional.empty()));
    }
}
