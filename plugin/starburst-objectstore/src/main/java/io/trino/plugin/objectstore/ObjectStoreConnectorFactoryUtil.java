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
package io.trino.plugin.objectstore;

import com.google.inject.Module;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.log.Logger;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;

import java.lang.annotation.Annotation;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class ObjectStoreConnectorFactoryUtil
{
    private static final Logger log = Logger.get(ObjectStoreConnectorFactoryUtil.class);

    private ObjectStoreConnectorFactoryUtil() {}

    public static Connector completeConnectorFuture(Future<Connector> connectorFuture)
    {
        try {
            return connectorFuture.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Thread interrupted", e);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof ApplicationConfigurationException configurationException) {
                throw configurationException;
            }
            log.warn(e, "Error while getting result of one of ObjectStore connector's futures");
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error instantiating connector", e.getCause());
        }
    }

    public static Module connectorModule(Class<? extends Annotation> annotation, Connector connector)
    {
        return binder -> {
            binder.bind(Connector.class).annotatedWith(annotation).toInstance(connector);
            binder.bind(ConnectorSplitManager.class).annotatedWith(annotation).toInstance(connector.getSplitManager());
            binder.bind(ConnectorPageSinkProvider.class).annotatedWith(annotation).toInstance(connector.getPageSinkProvider());
            binder.bind(ConnectorNodePartitioningProvider.class).annotatedWith(annotation).toInstance(connector.getNodePartitioningProvider());

            if (annotation == ForIceberg.class) {
                binder.bind(ConnectorPageSourceProviderFactory.class).annotatedWith(annotation).toInstance(connector.getPageSourceProviderFactory());
            }
            else {
                binder.bind(ConnectorPageSourceProvider.class).annotatedWith(annotation).toInstance(connector.getPageSourceProvider());
            }
        };
    }

    public static <T> Supplier<T> usingTracing(Tracer tracer, String spanName, Supplier<T> function)
    {
        Span registerQueryCatalogsSpan = tracer.spanBuilder(spanName)
                .startSpan();
        return () -> {
            try (var _ = registerQueryCatalogsSpan.makeCurrent()) {
                return function.get();
            }
            catch (Exception e) {
                registerQueryCatalogsSpan.setStatus(ERROR, e.getMessage());
                registerQueryCatalogsSpan.recordException(e, Attributes.of(booleanKey("exception.escaped"), true));
                throw e;
            }
            finally {
                registerQueryCatalogsSpan.end();
            }
        };
    }

    public static <T> Callable<T> toCallable(Supplier<T> supplier)
    {
        return supplier::get;
    }
}
