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
package io.starburst.stargate.tablemaintenance;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.FixedCheckpointInterval;

import java.time.LocalDateTime;
import java.util.Optional;

import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.extractOptimizeCheckpointFromQuery;
import static java.util.Objects.requireNonNull;

@JsonTypeName("fileModifiedTimeBased")
public record FileModifiedTimeBasedOptimizeStatus(
        Optional<LocalDateTime> latestOptimizeCheckpoint,
        OptimizeCheckpointInterval latestUsedInterval)
        implements TableOptimizeStatus
{
    public static final FileModifiedTimeBasedOptimizeStatus DEFAULT = new FileModifiedTimeBasedOptimizeStatus(
            Optional.empty(),
            new FixedCheckpointInterval());

    public FileModifiedTimeBasedOptimizeStatus
    {
        requireNonNull(latestOptimizeCheckpoint, "latestOptimizeCheckpoint is null");
        requireNonNull(latestUsedInterval, "latestUsedInterval is null");
    }

    @Override
    public FileModifiedTimeBasedOptimizeStatus withLatestOptimizeQuery(String optimizeQuery)
    {
        Optional<LocalDateTime> checkpoint = extractOptimizeCheckpointFromQuery(optimizeQuery);
        if (checkpoint.isEmpty()) {
            return this;
        }
        return new FileModifiedTimeBasedOptimizeStatus(checkpoint, latestUsedInterval);
    }
}
