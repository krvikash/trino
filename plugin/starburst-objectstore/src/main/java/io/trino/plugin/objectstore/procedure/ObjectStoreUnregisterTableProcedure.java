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
import io.trino.plugin.objectstore.ForDelta;
import io.trino.plugin.objectstore.ForIceberg;
import io.trino.plugin.objectstore.ObjectStoreSessionProperties;
import io.trino.plugin.objectstore.TrinoSecurityControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import java.lang.invoke.MethodHandle;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public final class ObjectStoreUnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;

    private static final String PROCEDURE_NAME = "unregister_table";
    private static final String SYSTEM_SCHEMA = "system";

    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";

    static {
        try {
            UNREGISTER_TABLE = lookup().unreflect(ObjectStoreUnregisterTableProcedure.class.getMethod("unregisterTable", ConnectorAccessControl.class, ConnectorSession.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private final TrinoSecurityControl securityControl;
    private final Procedure icebergUnregisterTable;
    private final Procedure deltaUnregisterTable;
    private final ObjectStoreSessionProperties sessionProperties;

    @Inject
    public ObjectStoreUnregisterTableProcedure(
            TrinoSecurityControl securityControl,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector,
            ObjectStoreSessionProperties sessionProperties)
    {
        this.securityControl = requireNonNull(securityControl, "securityControl is null");
        icebergUnregisterTable = icebergConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals("unregister_table"))
                .collect(onlyElement());
        deltaUnregisterTable = deltaConnector.getProcedures().stream()
                .filter(procedure -> procedure.getName().equals("unregister_table"))
                .collect(onlyElement());
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                SYSTEM_SCHEMA,
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Argument(SCHEMA_NAME, VARCHAR),
                        new Argument(TABLE_NAME, VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(
            ConnectorAccessControl accessControl,
            ConnectorSession session,
            String schemaName,
            String tableName)
            throws Throwable
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            try {
                icebergUnregisterTable.getMethodHandle().invoke(accessControl, sessionProperties.unwrap(ICEBERG, session), schemaName, tableName);
                securityControl.tableDropped(session, schemaName, tableName);
                return;
            }
            catch (TrinoException e) {
                if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                    throw e;
                }
            }
            try {
                deltaUnregisterTable.getMethodHandle().invoke(accessControl, sessionProperties.unwrap(DELTA, session), schemaName, tableName);
                securityControl.tableDropped(session, schemaName, tableName);
                return;
            }
            catch (TrinoException e) {
                if (!isError(e, UNSUPPORTED_TABLE_TYPE)) {
                    throw e;
                }
            }
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table type");
        }
    }

    private static boolean isError(TrinoException e, ErrorCodeSupplier... errorCodes)
    {
        return Stream.of(errorCodes)
                .map(ErrorCodeSupplier::toErrorCode)
                .anyMatch(e.getErrorCode()::equals);
    }
}
