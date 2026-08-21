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
package io.trino.plugin.pinot.substitution;

import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import static java.util.Objects.requireNonNull;

/**
 * Column identity for Pinot materialized-view substitution.
 * <p>
 * Pinot columns are flat scalars addressed by name; the connector has no {@code applyProjection}, so
 * a sub-field extraction stays a {@code Project} above a whole-column scan rather than being pushed
 * into the column handle. The column name alone therefore fully identifies a column. Read-path
 * attributes carried on {@link io.trino.plugin.pinot.PinotColumnHandle} (data type, expression,
 * aliasing, aggregate pushdown) are deliberately excluded from identity.
 */
public record PinotColumnId(String columnName)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("PinotColumnId", 1);

    public PinotColumnId
    {
        requireNonNull(columnName, "columnName is null");
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
