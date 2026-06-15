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
package io.starburst.stargate.icehouse.catalog.glue;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class GlueTableOperationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(GlueClientFactory.class).in(SINGLETON);

        Multibinder<ExecutionInterceptor> executionInterceptorMultibinder =
                Multibinder.newSetBinder(binder, ExecutionInterceptor.class, ForGlueClient.class);
        executionInterceptorMultibinder.addBinding()
                .toProvider(TelemetryExecutionInterceptorProvider.class).in(SINGLETON);
    }

    private static class TelemetryExecutionInterceptorProvider
            implements Provider<ExecutionInterceptor>
    {
        private final OpenTelemetry openTelemetry;

        @Inject
        public TelemetryExecutionInterceptorProvider(OpenTelemetry openTelemetry)
        {
            this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        }

        @Override
        public ExecutionInterceptor get()
        {
            return AwsSdkTelemetry.builder(openTelemetry)
                    .setCaptureExperimentalSpanAttributes(true)
                    .setRecordIndividualHttpError(true)
                    .build()
                    .createExecutionInterceptor();
        }
    }
}
