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
package io.trino.spi.connector;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Connector-supplied properties that drive predicate-based incremental refresh
 * of a materialized view. Returned by
 * {@link ConnectorMetadata#getMaterializedViewIncrementalRefresh}; consumed by
 * the engine at analyzer time to inject a {@code WHERE col > (SELECT max(col)
 * FROM mv)} filter into the view's source query and to force the refresh into
 * {@link io.trino.spi.RefreshType#INCREMENTAL_COLUMN} mode.
 */
public record MaterializedViewIncrementalRefresh(String incrementalColumn)
{
    public MaterializedViewIncrementalRefresh
    {
        requireNonNull(incrementalColumn, "incrementalColumn is null");
        checkArgument(!incrementalColumn.isBlank(), "incrementalColumn is blank");
    }
}
