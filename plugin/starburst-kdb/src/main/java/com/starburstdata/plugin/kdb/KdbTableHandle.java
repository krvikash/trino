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
package com.starburstdata.plugin.kdb;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record KdbTableHandle(SchemaTableName schemaTableName, OptionalLong limit)
        implements ConnectorTableHandle
{
    public KdbTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(limit, "limit is null");
    }

    public KdbTableHandle withLimit(long limit)
    {
        return new KdbTableHandle(schemaTableName, OptionalLong.of(limit));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(schemaTableName.toString());
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        return builder.toString();
    }
}
