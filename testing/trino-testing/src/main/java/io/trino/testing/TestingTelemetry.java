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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.node.NodeInfo;
import io.airlift.tracing.TracingModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.tracing.TracingScopesModule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.opentelemetry.api.common.AttributeKey.booleanKey;

public class TestingTelemetry
{
    // This attribute was deprecated and removed from stable attributes
    public static final AttributeKey<Boolean> EXCEPTION_ESCAPED = booleanKey("exception.escaped");

    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    private final InMemorySpanExporter spanExporter;
    private final ReentrantLock lock = new ReentrantLock();

    private TestingTelemetry(String name)
    {
        this(name, ImmutableMap.of());
    }

    private TestingTelemetry(String name, Map<String, String> properties)
    {
        this.spanExporter = InMemorySpanExporter.create();
        Bootstrap bootstrap = new Bootstrap(new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                install(new TracingModule(name, "test-version"));
                install(new TracingScopesModule());
                newSetBinder(binder, SpanProcessor.class).addBinding().toInstance(SimpleSpanProcessor.create(spanExporter));
                binder.bind(NodeInfo.class).toInstance(new NodeInfo("development"));
            }
        });
        if (!properties.isEmpty()) {
            bootstrap.setRequiredConfigurationProperties(properties);
        }
        Injector injector = bootstrap.initialize();
        this.tracer = injector.getInstance(Tracer.class);
        this.openTelemetry = injector.getInstance(OpenTelemetry.class);
    }

    public Tracer getTracer()
    {
        return tracer;
    }

    public Tracer getTracer(String scopeName)
    {
        return openTelemetry.getTracer(scopeName);
    }

    public <R extends Exception> List<SpanData> captureSpans(CheckedRunnable<R> runnable)
            throws R
    {
        // TODO: is there a way to parallelize that?
        lock.lock();
        spanExporter.reset();
        runnable.run();
        List<SpanData> spans = ImmutableList.copyOf(spanExporter.getFinishedSpanItems());
        spanExporter.reset();
        lock.unlock();
        return spans;
    }

    public void reset()
    {
        spanExporter.reset();
    }

    public static <E extends Exception> void withTracing(Span span, CheckedRunnable<E> supplier)
            throws E
    {
        try (var _ = span.makeCurrent()) {
            supplier.run();
        }
        catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, t.getMessage());
            span.recordException(t, Attributes.of(EXCEPTION_ESCAPED, true));
            throw t;
        }
        finally {
            span.end();
        }
    }

    public interface CheckedRunnable<E extends Exception>
    {
        void run()
                throws E;
    }

    public static TestingTelemetry create(String name)
    {
        return new TestingTelemetry(name);
    }

    public static TestingTelemetry create(String name, Map<String, String> properties)
    {
        return new TestingTelemetry(name, properties);
    }
}
