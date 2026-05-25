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
package io.trino.split.remote;

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Span;
import io.trino.SessionRepresentation;
import io.trino.connector.CatalogHandle;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record CreateRemoteSplitsTaskRequest(
        String taskId,
        SessionRepresentation session,
        ConnectorTransactionHandle transaction,
        ConnectorTableHandle handle,
        Span span,
        CatalogHandle catalogHandle,
        Optional<CatalogProperties> catalogProperties,
        Set<ColumnHandle> dynamicFilterColumns,
        Constraint constraint)
{
    public CreateRemoteSplitsTaskRequest
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(session, "session is null");
        requireNonNull(transaction, "transaction is null");
        requireNonNull(handle, "handle is null");
        requireNonNull(span, "span is null");
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(catalogProperties, "catalogProperties is null");
        dynamicFilterColumns = ImmutableSet.copyOf(dynamicFilterColumns);
        requireNonNull(constraint, "constraint is null");
    }
}
