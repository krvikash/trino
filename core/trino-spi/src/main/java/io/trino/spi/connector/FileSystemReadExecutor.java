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
package io.trino.spi.connector;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * JVM-wide bounded executor connectors can use to run filesystem read tasks.
 * Concurrency is capped by the engine so multiple connectors share the same budget.
 */
public interface FileSystemReadExecutor
{
    <T> CompletableFuture<T> submit(Callable<T> task);
}
