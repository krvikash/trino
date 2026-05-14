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
package io.trino.operator.gpu.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface GpuExchanger
{
    /**
     * Sentinel for a sink whose exchange already finished. Returned by
     * {@code GpuLocalExchange.createSink} when all sources are done, so the writer can drop
     * remaining pages without checking finish state again.
     */
    GpuExchanger FINISHED = new GpuExchanger()
    {
        @Override
        public void accept(@Borrow GpuPage page) {}

        @Override
        public ListenableFuture<Void> waitForWriting()
        {
            return immediateVoidFuture();
        }
    };

    void accept(@Borrow GpuPage page);

    /**
     * Future completing when downstream buffers have headroom for more bytes.
     */
    ListenableFuture<Void> waitForWriting();
}
