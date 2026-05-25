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

import io.trino.spi.gpu.IoExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

// Runs each read synchronously on the calling thread; needs no shutdown.
public final class DirectIoExecutor
        implements IoExecutor
{
    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task)
    {
        try {
            return CompletableFuture.completedFuture(task.call());
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
