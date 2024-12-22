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
package io.trino.plugin.warp.storage.engine;

public interface ConnectorSync
{
    default boolean isDefaultCatalog()
    {
        throw new UnsupportedOperationException();
    }

    default boolean isCatalogReducedResources()
    {
        throw new UnsupportedOperationException();
    }

    default long getCatalogContext()
    {
        throw new UnsupportedOperationException();
    }

    default int allocReaderId()
    {
        throw new UnsupportedOperationException();
    }

    default void freeReaderId(int readerId)
    {
        throw new UnsupportedOperationException();
    }
}
