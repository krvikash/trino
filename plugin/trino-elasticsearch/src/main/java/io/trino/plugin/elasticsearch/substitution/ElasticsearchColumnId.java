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
package io.trino.plugin.elasticsearch.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Column identity for Elasticsearch materialized-view substitution.
 * <p>
 * Elasticsearch represents a column as a {@code path} into (possibly nested) object/document
 * fields; a top-level column has a single-element path, a nested sub-field a multi-element one
 * (the connector joins the path with '.' for the column name).
 * Read-path-only attributes (elasticsearchType, decoder, predicate support)
 * are deliberately excluded from identity.
 */
public record ElasticsearchColumnId(List<String> path)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("ElasticsearchColumnId", 1);

    public ElasticsearchColumnId
    {
        path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
