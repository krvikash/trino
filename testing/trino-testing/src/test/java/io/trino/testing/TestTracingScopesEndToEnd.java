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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that the {@code tracing.enabled-scopes} config property flows end-to-end through real
 * Guice wiring - {@link io.trino.tracing.TracingScopesModule} bound, {@code @Config("tracing.enabled-scopes")}
 * read, and {@link io.trino.tracing.TracingScopesInitializer} applied to a live SdkTracerProvider -
 * rather than only exercising the raw OpenTelemetry SDK APIs directly (as TestTracingScopes does) or
 * the initializer's constructor logic in isolation with hand-built OpenTelemetry objects (as
 * TestTracingScopesInitializer does).
 */
public class TestTracingScopesEndToEnd
{
    @Test
    public void testEnabledScopesPropertyFiltersSpansThroughGuiceWiring()
    {
        TestingTelemetry telemetry = TestingTelemetry.create(
                "test-tracing-scopes",
                ImmutableMap.of("tracing.enabled-scopes", "matching.scope"));

        Tracer matchingTracer = telemetry.getTracer("matching.scope");
        Tracer nonMatchingTracer = telemetry.getTracer("other.scope");

        List<SpanData> spans = telemetry.captureSpans(() -> {
            matchingTracer.spanBuilder("matching-op").startSpan().end();
            nonMatchingTracer.spanBuilder("other-op").startSpan().end();
        });

        assertThat(spans).hasSize(1);
        assertThat(spans.get(0).getName()).isEqualTo("matching-op");
    }
}
