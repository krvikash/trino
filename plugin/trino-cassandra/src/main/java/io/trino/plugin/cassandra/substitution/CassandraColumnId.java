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
package io.trino.plugin.cassandra.substitution;

import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.type.Type;

/**
 * Column identity for Cassandra materialized-view substitution.
 * <p>
 * Cassandra does not push projections or dereferences into the scan, so a column handle
 * always names a whole top-level column. The identity is therefore just the column name and
 * its Trino type; any sub-field expression (e.g. {@code json_extract_scalar} over a map read
 * back as a JSON string) lives in a projection above the scan and matches on the whole column.
 */
public record CassandraColumnId(String name, Type type)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("CassandraColumnId", 1);

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
