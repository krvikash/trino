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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.iceberg.catalog.MetastoreCacheInvalidator;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class FlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String PARAM_TABLE_NAME = "TABLE_NAME";

    private static final MethodHandle FLUSH_METADATA_CACHE;

    static {
        try {
            FLUSH_METADATA_CACHE = lookup().unreflect(FlushMetadataCacheProcedure.class.getMethod("flushMetadataCache", ConnectorSession.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final MetastoreCacheInvalidator cacheInvalidator;

    @Inject
    public FlushMetadataCacheProcedure(MetastoreCacheInvalidator cacheInvalidator)
    {
        this.cacheInvalidator = requireNonNull(cacheInvalidator, "cacheInvalidator is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null)),
                FLUSH_METADATA_CACHE.bindTo(this),
                true);
    }

    public void flushMetadataCache(ConnectorSession session, String schemaName, String tableName)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doFlushMetadataCache(session, Optional.ofNullable(schemaName), Optional.ofNullable(tableName));
        }
    }

    private void doFlushMetadataCache(ConnectorSession session, Optional<String> schemaName, Optional<String> tableName)
    {
        try {
            if (schemaName.isEmpty() && tableName.isEmpty()) {
                cacheInvalidator.invalidateCache(session);
            }
            else if (schemaName.isPresent() && tableName.isPresent()) {
                cacheInvalidator.invalidateCache(session, new SchemaTableName(schemaName.get(), tableName.get()));
            }
            else {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Illegal parameter set passed. Both schema_name and table_name must be provided, or neither.");
            }
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, e.getMessage(), e);
        }
    }
}
