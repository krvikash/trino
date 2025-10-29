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

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.connector.CatalogFailureHandler;
import io.trino.metadata.Catalog;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class TestingCatalogFailureHandler
        implements CatalogFailureHandler
{
    private final Deque<CatalogFailure> failures = new ConcurrentLinkedDeque<>();

    public void reset()
    {
        failures.clear();
    }

    public List<CatalogFailure> getFailures()
    {
        return ImmutableList.copyOf(failures);
    }

    @Override
    public void handleCatalogFailure(Catalog catalog, Throwable cause)
    {
        failures.addLast(new CatalogFailure(catalog, cause));
    }

    public record CatalogFailure(Catalog catalog, Throwable cause)
    {
        public CatalogFailure
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(cause, "cause is null");
        }
    }

    public static Module module()
    {
        return binder -> {
            binder.bind(TestingCatalogFailureHandler.class).in(SINGLETON);
            newSetBinder(binder, CatalogFailureHandler.class).addBinding().to(TestingCatalogFailureHandler.class).in(SINGLETON);
        };
    }
}
