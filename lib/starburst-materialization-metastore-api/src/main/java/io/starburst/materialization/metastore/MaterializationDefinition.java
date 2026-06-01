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
package io.starburst.materialization.metastore;

import io.starburst.materialization.ir.Output;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents a materialization definition.
 *
 * @param computationPlanRoot contains the description of the materialized computation.
 * @param storageTableId identifies the table where the materialized data is stored.
 * @param source describes how the computation was materialized. For now, it can only be a materialized view.
 * @param lastKnownFreshTime the last time the materialized data was refreshed.
 * @param gracePeriod how long after {@code lastKnownFreshTime} the materialization is still considered fresh for substitution. Optional.empty means no upper bound (always fresh once refreshed).
 */
public record MaterializationDefinition(
        Output computationPlanRoot,
        StorageTableId storageTableId,
        MaterializationSource source,
        Instant lastKnownFreshTime,
        Optional<Duration> gracePeriod)
{
    public MaterializationDefinition
    {
        requireNonNull(computationPlanRoot, "computationPlanRoot is null");
        requireNonNull(storageTableId, "storageTableId is null");
        requireNonNull(source, "source is null");
        requireNonNull(lastKnownFreshTime, "lastKnownFreshTime is null");
        requireNonNull(gracePeriod, "gracePeriod is null");
    }
}
