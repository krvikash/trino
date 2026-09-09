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

import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTracingScopesInitializer
{
    @Test
    public void testMasterSwitchOffIsNoop()
    {
        // OpenTelemetry.noop() is not an OpenTelemetrySdk - there is no SdkTracerProvider to
        // reach. Must not throw; the master tracing.enabled=false switch already made every
        // scope a no-op.
        new TracingScopesInitializer(OpenTelemetry.noop(), new TracingScopesConfig().setEnabledScopes(ImmutableList.of("some.scope")));
    }

    @Test
    public void testEmptyScopeListLeavesEverythingEnabled()
    {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .build();

        new TracingScopesInitializer(sdk, new TracingScopesConfig());

        sdk.getTracer("any.scope.at.all").spanBuilder("op").startSpan().end();
        assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    }

    @Test
    public void testConfiguredScopesAreSelectivelyEnabled()
    {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .build();

        new TracingScopesInitializer(sdk, new TracingScopesConfig().setEnabledScopes(ImmutableList.of("io.starburst.ai.agent")));

        sdk.getTracer("io.starburst.ai.agent").spanBuilder("agent-op").startSpan().end();
        sdk.getTracer("io.trino.execution").spanBuilder("query-op").startSpan().end();

        assertThat(exporter.getFinishedSpanItems()).hasSize(1);
        assertThat(exporter.getFinishedSpanItems().get(0).getName()).isEqualTo("agent-op");
    }
}
