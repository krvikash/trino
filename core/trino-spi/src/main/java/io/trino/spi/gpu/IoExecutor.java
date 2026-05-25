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
package io.trino.spi.gpu;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Executor a connector page source may use to offload blocking reads of split data off the
 * thread producing pages, for example to fetch several column chunks of a split in parallel.
 * <p>
 * A submitted task may queue before it runs. Intended for blocking reads; CPU-bound or
 * long-running work does not belong here, and work run here is not attributed to the query.
 */
public interface IoExecutor
{
    <T> CompletableFuture<T> submit(Callable<T> task);
}
