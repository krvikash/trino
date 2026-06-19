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
package io.trino.operator.gpu.scan;

import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToDevice;
import io.trino.operator.gpu.GpuOperation;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.gpu.ConnectorGpuPageSource;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public class ConnectorGpuPageSourceAdapter
        implements ConnectorGpuPageSource
{
    private final ConnectorPageSource source;
    private final BufferPages bufferPages;
    private final GpuOperation output;
    private boolean sourceExhausted;

    public ConnectorGpuPageSourceAdapter(GpuOperation.Context gpuOperationContext, ConnectorPageSource source, List<Type> types)
    {
        this.source = requireNonNull(source, "source is null");
        this.bufferPages = new BufferPages();
        this.output = new CopyToDevice(
                gpuOperationContext,
                bufferPages,
                types,
                IntStream.range(0, types.size())
                        .boxed()
                        .collect(toImmutableSet()));
    }

    @Override
    public @Move Result readNext()
    {
        @Own GpuOperation.Result result = output.execute();
        return switch (result) {
            case GpuOperation.Blocked(var future) -> new Blocked(toCompletableFuture(future));
            case GpuOperation.Data(var memory, var gpuPage) -> new Data(memory, gpuPage);
            case GpuOperation.Finished() -> new Finished();
            case GpuOperation.Yielded() -> {
                if (sourceExhausted || !bufferPages.needsInput()) {
                    yield new Yielded();
                }
                CompletableFuture<?> blocked = source.isBlocked();
                if (!blocked.isDone()) {
                    yield new Blocked(blocked.thenAccept(_ -> {}));
                }
                if (source.isFinished()) {
                    bufferPages.noMoreInput();
                    sourceExhausted = true;
                    yield new Yielded();
                }
                SourcePage page = source.getNextSourcePage();
                if (page != null) {
                    bufferPages.addInput(page.getPage());
                }
                yield new Yielded();
            }
        };
    }

    @Override
    public void close()
    {
        try (var closer = AutoCloseableCloser.create()) {
            closer.register(source);
            closer.register(output); // closes bufferPages too
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
