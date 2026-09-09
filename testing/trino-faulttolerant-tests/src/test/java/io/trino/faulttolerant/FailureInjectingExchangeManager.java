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
package io.trino.faulttolerant;

import io.airlift.slice.Slice;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeSourceHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import io.trino.spi.metrics.Metrics;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE;
import static java.util.Objects.requireNonNull;

/**
 * Delegating ExchangeManager that wraps another manager and, while the injector it was created with
 * is armed, injects EXCHANGE_DATA_UNRECOVERABLE failures into ExchangeSource.read() calls.
 */
public class FailureInjectingExchangeManager
        implements ExchangeManager
{
    private final ExchangeManager delegate;
    private final ExchangeFailureInjector failureInjector;

    public FailureInjectingExchangeManager(ExchangeManager delegate, ExchangeFailureInjector failureInjector)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.failureInjector = requireNonNull(failureInjector, "failureInjector is null");
    }

    @Override
    public Exchange createExchange(ExchangeContext context, int outputPartitionCount, boolean preserveOrderWithinPartition)
    {
        failureInjector.exchangeCreated(context.getExchangeId());
        return delegate.createExchange(context, outputPartitionCount, preserveOrderWithinPartition);
    }

    @Override
    public ExchangeSink createSink(ExchangeSinkInstanceHandle handle)
    {
        return delegate.createSink(handle);
    }

    @Override
    public ExchangeSource createSource()
    {
        return new FailureInjectingExchangeSource(delegate.createSource(), failureInjector);
    }

    @Override
    public boolean supportsConcurrentReadAndWrite()
    {
        return delegate.supportsConcurrentReadAndWrite();
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    private static class FailureInjectingExchangeSource
            implements ExchangeSource
    {
        private final ExchangeSource delegate;
        private final ExchangeFailureInjector failureInjector;
        private volatile ExchangeId exchangeId;

        private FailureInjectingExchangeSource(ExchangeSource delegate, ExchangeFailureInjector failureInjector)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.failureInjector = requireNonNull(failureInjector, "failureInjector is null");
        }

        @Override
        public void addSourceHandles(List<ExchangeSourceHandle> handles)
        {
            if (exchangeId == null && !handles.isEmpty()) {
                exchangeId = ((FileSystemExchangeSourceHandle) handles.getFirst()).getExchangeId();
            }
            delegate.addSourceHandles(handles);
        }

        @Override
        public void noMoreSourceHandles()
        {
            delegate.noMoreSourceHandles();
        }

        @Override
        public void setOutputSelector(ExchangeSourceOutputSelector selector)
        {
            delegate.setOutputSelector(selector);
        }

        @Override
        public CompletableFuture<Void> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public Slice read()
        {
            if (exchangeId != null && failureInjector.shouldFailRead(exchangeId)) {
                throw new TrinoException(EXCHANGE_DATA_UNRECOVERABLE, "Injected exchange data loss for testing");
            }
            return delegate.read();
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public Optional<Metrics> getMetrics()
        {
            return delegate.getMetrics();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
