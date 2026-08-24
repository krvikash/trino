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
package io.trino.plugin.redis.substitution;

import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import static java.util.Objects.requireNonNull;

public record RedisColumnId(String columnName)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("RedisColumnId", 1);

    public RedisColumnId
    {
        requireNonNull(columnName, "columnName is null");
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
