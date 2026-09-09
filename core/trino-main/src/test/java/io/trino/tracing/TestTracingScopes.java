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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.common.internal.ScopeConfiguratorBuilder;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.internal.SdkTracerProviderUtil;
import io.opentelemetry.sdk.trace.internal.TracerConfig;
import org.junit.jupiter.api.Test;

import static io.opentelemetry.sdk.common.internal.ScopeConfiguratorBuilder.nameMatchesGlob;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTracingScopes
{
    /**
     * Pins that a Tracer obtained BEFORE the configurator is applied still gets updated —
     * this is what makes a hand-rolled cache of previously-vended Tracer instances unnecessary.
     * Also pins nameMatchesGlob matching/non-matching behavior.
     */
    @Test
    public void testConfiguratorUpdatesAlreadyVendedTracers()
    {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();

        Tracer disabledScopeTracer = provider.get("disabled.scope");
        Tracer enabledScopeTracer = provider.get("enabled.scope");

        ScopeConfiguratorBuilder<TracerConfig> builder = TracerConfig.configuratorBuilder()
                .setDefault(TracerConfig.disabled());
        builder.addCondition(nameMatchesGlob("enabled.*"), TracerConfig.enabled());
        SdkTracerProviderUtil.setTracerConfigurator(provider, builder.build());

        disabledScopeTracer.spanBuilder("disabled-op").startSpan().end();
        enabledScopeTracer.spanBuilder("enabled-op").startSpan().end();

        assertThat(exporter.getFinishedSpanItems()).hasSize(1);
        assertThat(exporter.getFinishedSpanItems().get(0).getName()).isEqualTo("enabled-op");
    }

    /**
     * Pins the trace-continuity property: a real span in a re-enabled scope, created after
     * passing through a disabled scope, reports the SAME trace ID as the last real ancestor —
     * not a new, disconnected trace. This is the PropagatedSpan pass-through behavior a disabled
     * scope shares with TracerProvider.noop().
     */
    @Test
    public void testDisabledScopePropagatesAncestorTraceId()
    {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();

        ScopeConfiguratorBuilder<TracerConfig> builder = TracerConfig.configuratorBuilder()
                .setDefault(TracerConfig.disabled());
        builder.addCondition(nameMatchesGlob("root.scope"), TracerConfig.enabled());
        builder.addCondition(nameMatchesGlob("leaf.scope"), TracerConfig.enabled());
        SdkTracerProviderUtil.setTracerConfigurator(provider, builder.build());

        Tracer rootTracer = provider.get("root.scope");
        Tracer middleTracer = provider.get("middle.scope"); // not matched by any condition -> disabled
        Tracer leafTracer = provider.get("leaf.scope");

        Span root = rootTracer.spanBuilder("root").startSpan();
        try (var ignored = root.makeCurrent()) {
            Span middle = middleTracer.spanBuilder("middle").startSpan();
            try (var ignoredMiddle = middle.makeCurrent()) {
                Span leaf = leafTracer.spanBuilder("leaf").startSpan();
                assertThat(leaf.getSpanContext().getTraceId()).isEqualTo(root.getSpanContext().getTraceId());
                leaf.end();
            }
            middle.end();
        }
        root.end();

        // root and leaf are recorded; middle, from a disabled scope, is not.
        assertThat(exporter.getFinishedSpanItems()).hasSize(2);
    }
}
