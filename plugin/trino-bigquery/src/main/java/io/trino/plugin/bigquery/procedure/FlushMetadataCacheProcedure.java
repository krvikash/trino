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
package io.trino.plugin.bigquery.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.bigquery.BigQueryClientFactory;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class FlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_METADATA_CACHE;

    static {
        try {
            FLUSH_METADATA_CACHE = lookup().unreflect(FlushMetadataCacheProcedure.class.getMethod("flushMetadataCache"));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final BigQueryClientFactory bigQueryClientFactory;

    @Inject
    public FlushMetadataCacheProcedure(
            BigQueryClientFactory bigQueryClientFactory)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "flush_metadata_cache",
                ImmutableList.of(),
                FLUSH_METADATA_CACHE.bindTo(this));
    }

    public void flushMetadataCache()
    {
        bigQueryClientFactory.flushCache();
    }
}
