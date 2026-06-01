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
package io.trino.tracing;

import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.metastore.StorageTableId;
import io.starburst.server.substitution.SubstitutionMetadata;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.Optional;
import java.util.Set;

import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class TracingSubstitutionMetadata
        implements SubstitutionMetadata
{
    private final Tracer tracer;
    private final SubstitutionMetadata delegate;

    @Inject
    public TracingSubstitutionMetadata(Tracer tracer, @ForTracing SubstitutionMetadata delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Optional<TableId> getTableId(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getTableId", tableHandle.catalogHandle());
        try (var _ = scopedSpan(span)) {
            return delegate.getTableId(session, tableHandle);
        }
    }

    @Override
    public Optional<StorageTableId> getStorageTableId(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getStorageTableId", tableHandle.catalogHandle());
        try (var _ = scopedSpan(span)) {
            return delegate.getStorageTableId(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorColumnId> getColumnId(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        Span span = startSpan("getColumnId", tableHandle.catalogHandle());
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnId(session, tableHandle, columnHandle);
        }
    }

    @Override
    public boolean tableHandleMatchesId(Session session, TableHandle queryTable, TableId candidateTable)
    {
        Span span = startSpan("tableHandleMatchesId", queryTable.catalogHandle());
        try (var _ = scopedSpan(span)) {
            return delegate.tableHandleMatchesId(session, queryTable, candidateTable);
        }
    }

    @Override
    public Set<ConnectorIdVersion> tableIdVersions(CatalogHandle catalogHandle)
    {
        Span span = startSpan("tableIdVersions", catalogHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.tableIdVersions(catalogHandle);
        }
    }

    @Override
    public Set<ConnectorIdVersion> columnIdVersions(CatalogHandle catalogHandle)
    {
        Span span = startSpan("columnIdVersions", catalogHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.columnIdVersions(catalogHandle);
        }
    }

    private Span startSpan(String methodName, CatalogHandle catalogHandle)
    {
        return tracer.spanBuilder("SubstitutionMetadata." + methodName)
                .setAttribute(TrinoAttributes.CATALOG, catalogHandle.getCatalogName().toString())
                .startSpan();
    }
}
