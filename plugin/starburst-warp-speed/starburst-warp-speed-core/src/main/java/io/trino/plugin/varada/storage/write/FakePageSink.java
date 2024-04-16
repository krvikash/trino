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
package io.trino.plugin.varada.storage.write;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class FakePageSink
        implements ConnectorPageSink
{
    private static final Logger logger = Logger.get(FakePageSink.class);

    private final RowGroupKey rowGroupKey;
    private final AtomicInteger counter;

    public FakePageSink(RowGroupKey rowGroupKey, AtomicInteger counter)
    {
        this.rowGroupKey = rowGroupKey;
        this.counter = counter;
        logger.info("start FakePageSink value=%s, key=%s", this.counter.getAndIncrement(), rowGroupKey);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        logger.info("counter is %s, key=%s", counter.getAndDecrement(), rowGroupKey);
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    @Override
    public void abort()
    {
        logger.info("ABORT counter is %s. key=%s", counter.getAndDecrement(), rowGroupKey);
    }
}
