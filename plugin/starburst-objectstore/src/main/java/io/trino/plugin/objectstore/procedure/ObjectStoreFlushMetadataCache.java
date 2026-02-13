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
package io.trino.plugin.objectstore.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.deltalake.metastore.NotADeltaLakeTableException;
import io.trino.plugin.objectstore.ForDelta;
import io.trino.plugin.objectstore.ForHive;
import io.trino.plugin.objectstore.ForHudi;
import io.trino.plugin.objectstore.ForIceberg;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;

public final class ObjectStoreFlushMetadataCache
        implements Provider<Procedure>
{
    private static final String SYSTEM_SCHEMA = "system";
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String PARAM_TABLE_NAME = "TABLE_NAME";
    private static final String PARAM_PARTITION_COLUMNS = "PARTITION_COLUMNS";
    private static final String PARAM_PARTITION_VALUES = "PARTITION_VALUES";

    private static final MethodHandle FLUSH_METADATA_CACHE;

    static {
        try {
            FLUSH_METADATA_CACHE = lookup().unreflect(ObjectStoreFlushMetadataCache.class.getMethod(
                    "flushMetadataCache", ConnectorSession.class, String.class, String.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private final Procedure hiveFlushMetadata;
    private final Procedure icebergFlushMetadata;
    private final Procedure deltaFlushMetadata;

    @Inject
    public ObjectStoreFlushMetadataCache(
            @ForHive Connector hiveConnector,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector,
            @ForHudi Connector hudiConnector)
    {
        this.hiveFlushMetadata = hiveConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals(PROCEDURE_NAME))
                .collect(onlyElement());

        this.icebergFlushMetadata = icebergConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals(PROCEDURE_NAME))
                .collect(onlyElement());

        this.deltaFlushMetadata = deltaConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals(PROCEDURE_NAME))
                .collect(onlyElement());

        verify(
                hudiConnector.getProcedures().stream()
                        .noneMatch(procedure -> procedure.getName().equals(PROCEDURE_NAME)),
                "Unexpected %s procedure in Hudi",
                PROCEDURE_NAME);
    }

    @Override
    public Procedure get()
    {
        List<Procedure.Argument> arguments = ImmutableList.<Procedure.Argument>builder()
                .add(new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null))
                .add(new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null))
                .add(new Procedure.Argument(PARAM_PARTITION_COLUMNS, new ArrayType(VARCHAR), false, null))
                .add(new Procedure.Argument(PARAM_PARTITION_VALUES, new ArrayType(VARCHAR), false, null))
                .build();

        Map<String, Procedure.Argument> argumentsByName = uniqueIndex(arguments, Procedure.Argument::getName);

        for (Procedure delegate : List.of(hiveFlushMetadata, icebergFlushMetadata, deltaFlushMetadata)) {
            for (Procedure.Argument delegateArgument : delegate.getArguments()) {
                Procedure.Argument actual = argumentsByName.get(delegateArgument.getName());
                checkState(actual != null, "Argument not exposed: %s", delegateArgument.getName());
                checkState(
                        actual.getType().equals(delegateArgument.getType()) &&
                                Objects.equals(actual.getDefaultValue(), delegateArgument.getDefaultValue()) &&
                                actual.isRequired() == delegateArgument.isRequired(),
                        "Different argument than in delegate: %s vs %s",
                        actual,
                        delegateArgument);
            }
        }

        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                arguments,
                FLUSH_METADATA_CACHE.bindTo(this),
                true);
    }

    public void flushMetadataCache(
            @Nullable ConnectorSession session,
            @Nullable String schemaName,
            @Nullable String tableName,
            @Nullable List<String> partitionColumns,
            @Nullable List<String> partitionValues)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            icebergFlushMetadata.getMethodHandle().invoke(session, schemaName, tableName);
            // hiveFlushMetadata needs to be invoked regardless of table type, since it flushes also table listings
            hiveFlushMetadata.getMethodHandle().invoke(session, schemaName, tableName, partitionColumns, partitionValues);
            try {
                deltaFlushMetadata.getMethodHandle().invoke(schemaName, tableName);
            }
            catch (NotADeltaLakeTableException ignore) {
                // Sure, we didn't check whether this is a Delta table.
            }
            catch (TableNotFoundException ignore) {
                // Hive's procedure can be called with invalid table name, so we need to ignore this.
            }
        }
        catch (Throwable e) {
            throwIfUnchecked(e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
