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
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.internal.ScopeConfiguratorBuilder;
import io.opentelemetry.sdk.trace.internal.SdkTracerProviderUtil;
import io.opentelemetry.sdk.trace.internal.TracerConfig;

import static io.opentelemetry.sdk.common.internal.ScopeConfiguratorBuilder.nameMatchesGlob;
import static java.util.Objects.requireNonNull;

/**
 * Applies {@link TracingScopesConfig#getEnabledScopes()} to the live {@link OpenTelemetrySdk}'s
 * SdkTracerProvider at startup, using the OpenTelemetry SDK's own per-instrumentation-scope
 * enable/disable mechanism. A scope not matching any configured glob is disabled: it still
 * propagates trace context (see TestTracingScopes) but produces no recorded spans.
 * <p>
 * Deliberately lets any failure reaching the internal SdkTracerProviderUtil API propagate out
 * of the constructor and fail server bootstrap, rather than silently leaving every scope enabled
 * — an operator who asked for "agent only" getting a full-firehose export by default is the wrong
 * failure direction for a property that partly exists to limit what leaves the process.
 */
public class TracingScopesInitializer
{
    private static final Logger log = Logger.get(TracingScopesInitializer.class);

    @Inject
    public TracingScopesInitializer(OpenTelemetry openTelemetry, TracingScopesConfig config)
    {
        requireNonNull(openTelemetry, "openTelemetry is null");
        requireNonNull(config, "config is null");
        if (config.getEnabledScopes().isEmpty()) {
            return;
        }
        if (!(openTelemetry instanceof OpenTelemetrySdk sdk)) {
            // tracing.enabled=false binds OpenTelemetry.noop(): every scope is already a no-op.
            log.warn("tracing.enabled-scopes is set to %s but tracing.enabled is not true; the property has no effect", config.getEnabledScopes());
            return;
        }
        ScopeConfiguratorBuilder<TracerConfig> builder = TracerConfig.configuratorBuilder()
                .setDefault(TracerConfig.disabled());
        for (String scope : config.getEnabledScopes()) {
            builder.addCondition(nameMatchesGlob(scope), TracerConfig.enabled());
        }
        SdkTracerProviderUtil.setTracerConfigurator(sdk.getSdkTracerProvider(), builder.build());
        log.info("Tracing enabled only for instrumentation scopes matching: %s", config.getEnabledScopes());
    }
}
