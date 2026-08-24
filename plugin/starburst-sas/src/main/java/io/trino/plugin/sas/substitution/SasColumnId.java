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
package io.trino.plugin.sas.substitution;

import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import static java.util.Objects.requireNonNull;

/**
 * Column identity for SAS materialized-view substitution. SAS columns are flat scalars discovered
 * from the {@code .sas7bdat} file, so a column is identified by its name alone. The connector does
 * not push dereferences into the scan (nested data is exposed as JSON text in a VARCHAR column and
 * extracted by a projection above the scan), so no sub-field path is part of the identity. The
 * column type and ordinal position are read-path attributes owned by the engine and excluded.
 */
public record SasColumnId(String columnName)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("SasColumnId", 1);

    public SasColumnId
    {
        requireNonNull(columnName, "columnName is null");
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
