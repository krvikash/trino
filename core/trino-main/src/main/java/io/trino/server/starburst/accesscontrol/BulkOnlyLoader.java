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
package io.trino.server.starburst.accesscontrol;

import com.google.common.cache.CacheLoader;

import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class BulkOnlyLoader<K, V>
        extends CacheLoader<K, V>
{
    public static <K, V> CacheLoader<K, V> of(Function<Iterable<? extends K>, Map<K, V>> loader)
    {
        return new BulkOnlyLoader<>(loader);
    }

    private final Function<Iterable<? extends K>, Map<K, V>> loader;

    public BulkOnlyLoader(Function<Iterable<? extends K>, Map<K, V>> loader)
    {
        this.loader = requireNonNull(loader, "loader is null");
    }

    @Override
    public V load(K key)
    {
        // Only loadAll should be used
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys)
    {
        return loader.apply(keys);
    }
}
