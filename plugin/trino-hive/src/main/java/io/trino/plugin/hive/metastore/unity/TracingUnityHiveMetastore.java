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
package io.trino.plugin.hive.metastore.unity;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.tracing.TracingHiveMetastore;
import io.trino.spi.connector.SchemaTableName;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;

import java.util.Optional;

import static io.trino.metastore.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

public class TracingUnityHiveMetastore
        extends TracingHiveMetastore
        implements UnityMetastore
{
    private final Tracer tracer;
    private final UnityHiveMetastore delegate;

    public TracingUnityHiveMetastore(Tracer tracer, UnityHiveMetastore delegate)
    {
        super(tracer, delegate);
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public StagedCommitsInfo loadStagedCommitsInfo(String tableId, String tableLocation, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        Span span = tracer.spanBuilder("HiveMetastore.getStagedCommitsInfo")
                .startSpan();
        return withTracing(span, () -> delegate.loadStagedCommitsInfo(tableId, tableLocation, startVersion, endVersion));
    }

    @Override
    public void commitStagedCommits(CommitRequest commitStagedRequest)
    {
        Span span = tracer.spanBuilder("HiveMetastore.commitStagedCommits")
                .startSpan();
        withTracing(span, () -> delegate.commitStagedCommits(commitStagedRequest));
    }

    @Override
    public DeltaCredentialsResponse getTemporaryTableCredentials(SchemaTableName schemaTableName, DeltaCredentialOperation operation)
    {
        Span span = tracer.spanBuilder("UnityHiveMetastore.getTemporaryTableCredentials")
                .startSpan();
        return withTracing(span, () -> delegate.getTemporaryTableCredentials(schemaTableName, operation));
    }

    @Override
    public TemporaryCredentials getTemporaryPathCredentials(String tableLocation, PathOperation operation)
    {
        Span span = tracer.spanBuilder("UnityHiveMetastore.getTemporaryPathCredentials")
                .startSpan();
        return withTracing(span, () -> delegate.getTemporaryPathCredentials(tableLocation, operation));
    }
}
