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
package io.trino.plugin.opensearch.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Column identity for OpenSearch materialized-view substitution.
 * <p>
 * OpenSearch represents a column as a {@code path} into (possibly nested) object/document
 * fields; a top-level column has a single-element path, a nested sub-field a multi-element one
 * (the connector joins the path with '.' for the column name). A dereference into a struct
 * sub-field is pushed down into the scan as a longer path (see
 * {@code OpenSearchMetadata.applyProjection}), so the path is load-bearing for identity: the
 * base-side sub-field scan and the MV-side scan must produce the same id. Opaque JSON columns
 * stay whole-column scans (a single-element path) with the sub-field extraction living in a
 * Project above the scan. Read-path-only attributes (opensearchType, decoder, predicate
 * support) are deliberately excluded from identity.
 */
public record OpenSearchColumnId(List<String> path)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("OpenSearchColumnId", 1);

    public OpenSearchColumnId
    {
        path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
