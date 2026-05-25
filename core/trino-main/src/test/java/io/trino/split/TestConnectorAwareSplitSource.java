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
package io.trino.split;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

final class TestConnectorAwareSplitSource
{
    @Test
    void testGetNextBatchDoesNotBlockOnAsyncWaitTimeout()
    {
        SettableFuture<Long> waitTimeout = SettableFuture.create();
        ConnectorAwareSplitSource splitSource = new ConnectorAwareSplitSource(
                TEST_CATALOG_HANDLE, new EmptySplitSource(), DynamicFilter.EMPTY, waitTimeout);

        // A remote source only learns its wait timeout from the create round-trip. getNextBatch must
        // chain on that future rather than block a scheduler thread, so it returns a pending future.
        ListenableFuture<SplitSource.SplitBatch> batch = splitSource.getNextBatch(10);
        assertThat(batch.isDone()).isFalse();

        // Once the timeout resolves, the batch is produced.
        waitTimeout.set(0L);
        assertThat(batch.isDone()).isTrue();
    }

    private static final class EmptySplitSource
            implements ConnectorSplitSource
    {
        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            return completedFuture(List.of());
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public void close() {}
    }
}
