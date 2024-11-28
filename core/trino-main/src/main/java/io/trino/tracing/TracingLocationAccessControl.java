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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.LocationAccessControl;

import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class TracingLocationAccessControl
        implements LocationAccessControl
{
    private final Tracer tracer;
    private final LocationAccessControl delegate;

    @Inject
    public TracingLocationAccessControl(Tracer tracer, @ForTracing LocationAccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @VisibleForTesting
    public LocationAccessControl getDelegate()
    {
        return delegate;
    }

    @Override
    public void checkCanUseLocation(ConnectorIdentity identity, String location, String queryId)
    {
        Span span = startSpan("checkCanUseLocation");
        try (var _ = scopedSpan(span)) {
            delegate.checkCanUseLocation(identity, location, queryId);
        }
    }

    private Span startSpan(String methodName)
    {
        return tracer.spanBuilder("LocationAccessControl." + methodName)
                .startSpan();
    }
}
