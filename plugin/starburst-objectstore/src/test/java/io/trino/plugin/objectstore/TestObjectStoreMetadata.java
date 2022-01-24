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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hudi.HudiMetadata;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.util.function.Predicate.not;

public class TestObjectStoreMetadata
{
    @Test
    public void testEverythingImplemented()
            throws Exception
    {
        assertAllMethodsOverridden(ConnectorMetadata.class, ObjectStoreMetadata.class, ImmutableSet.<Method>builder()
                // Do not require implementation of deprecated methods, as they are not called by the engine
                .addAll(Stream.of(ConnectorMetadata.class.getMethods())
                        .filter(method -> method.isAnnotationPresent(Deprecated.class))
                        // ... out of deprecated methods, these are implemented for readability and to prevent accidentally calling the default implementation
                        .filter(not(ConnectorMetadata.class.getMethod("streamTableColumns", ConnectorSession.class, SchemaTablePrefix.class)::equals))
                        .filter(not(ConnectorMetadata.class.getMethod("listTableColumns", ConnectorSession.class, SchemaTablePrefix.class)::equals))
                        .filter(not(ConnectorMetadata.class.getMethod("delegateMaterializedViewRefreshToConnector", ConnectorSession.class, SchemaTableName.class)::equals))
                        .collect(toImmutableList()))
                // Do not require implementation of methods that are not implemented by any of delegate connectors
                .addAll(Stream.of(ConnectorMetadata.class.getMethods())
                        .filter(method -> !overrides(HiveMetadata.class, method) &&
                                !overrides(IcebergMetadata.class, method) &&
                                !overrides(DeltaLakeMetadata.class, method) &&
                                !overrides(HudiMetadata.class, method))
                        // Method applyTableScanRedirect used to be overridden in DeltaLakeMetadata (and therefore filtered out above) until
                        // https://github.com/trinodb/trino/pull/24610/files#diff-79f367e47040dda32ac81ecec46b27cfd7cc822858bc8c31fe435a15a47446dcL3580
                        .filter(not(ConnectorMetadata.class.getMethod("applyTableScanRedirect", ConnectorSession.class, ConnectorTableHandle.class)::equals))
                        // ... out of deprecated methods, these are implemented for readability and to prevent accidentally calling the default implementation
                        .filter(not(ConnectorMetadata.class.getMethod("beginMerge", ConnectorSession.class, ConnectorTableHandle.class, Map.class, RetryMode.class)::equals))
                        .collect(toImmutableList()))
                // Supported in HiveMetadata only, but only with extension point that Galaxy does not use
                .add(ConnectorMetadata.class.getMethod("refreshMaterializedView", ConnectorSession.class, SchemaTableName.class))
                // TODO https://github.com/starburstdata/galaxy-trino/issues/1432 Implement getRelationTypes method in ObjectStoreMetadata
                .add(ConnectorMetadata.class.getMethod("getRelationTypes", ConnectorSession.class, Optional.class))
                // Not implemented, not applicable in Galaxy
                .add(ConnectorMetadata.class.getMethod("listRoles", ConnectorSession.class))
                .add(ConnectorMetadata.class.getMethod("listApplicableRoles", ConnectorSession.class, TrinoPrincipal.class))
                .add(ConnectorMetadata.class.getMethod("listEnabledRoles", ConnectorSession.class))
                .add(ConnectorMetadata.class.getMethod("roleExists", ConnectorSession.class, String.class))
                .add(ConnectorMetadata.class.getMethod("createRole", ConnectorSession.class, String.class, Optional.class))
                .add(ConnectorMetadata.class.getMethod("dropRole", ConnectorSession.class, String.class))
                .add(ConnectorMetadata.class.getMethod("grantRoles", ConnectorSession.class, Set.class, Set.class, boolean.class, Optional.class))
                .add(ConnectorMetadata.class.getMethod("revokeRoles", ConnectorSession.class, Set.class, Set.class, boolean.class, Optional.class))
                .add(ConnectorMetadata.class.getMethod("listRoleGrants", ConnectorSession.class, TrinoPrincipal.class))
                .add(ConnectorMetadata.class.getMethod("redirectTable", ConnectorSession.class, SchemaTableName.class))
                .add(ConnectorMetadata.class.getMethod("setSchemaAuthorization", ConnectorSession.class, String.class, TrinoPrincipal.class))
                .add(ConnectorMetadata.class.getMethod("getSchemaOwner", ConnectorSession.class, String.class))
                .add(ConnectorMetadata.class.getMethod("grantSchemaPrivileges", ConnectorSession.class, String.class, Set.class, TrinoPrincipal.class, boolean.class))
                .add(ConnectorMetadata.class.getMethod("revokeSchemaPrivileges", ConnectorSession.class, String.class, Set.class, TrinoPrincipal.class, boolean.class))
                .add(ConnectorMetadata.class.getMethod("setTableAuthorization", ConnectorSession.class, SchemaTableName.class, TrinoPrincipal.class))
                .add(ConnectorMetadata.class.getMethod("grantTablePrivileges", ConnectorSession.class, SchemaTableName.class, Set.class, TrinoPrincipal.class, boolean.class))
                .add(ConnectorMetadata.class.getMethod("revokeTablePrivileges", ConnectorSession.class, SchemaTableName.class, Set.class, TrinoPrincipal.class, boolean.class))
                .add(ConnectorMetadata.class.getMethod("listTablePrivileges", ConnectorSession.class, SchemaTablePrefix.class))
                .add(ConnectorMetadata.class.getMethod("setViewAuthorization", ConnectorSession.class, SchemaTableName.class, TrinoPrincipal.class))
                .add(ConnectorMetadata.class.getMethod("listLanguageFunctions", ConnectorSession.class, String.class))
                .add(ConnectorMetadata.class.getMethod("getLanguageFunctions", ConnectorSession.class, SchemaFunctionName.class))
                .add(ConnectorMetadata.class.getMethod("languageFunctionExists", ConnectorSession.class, SchemaFunctionName.class, String.class))
                .add(ConnectorMetadata.class.getMethod("createLanguageFunction", ConnectorSession.class, SchemaFunctionName.class, LanguageFunction.class, boolean.class))
                .add(ConnectorMetadata.class.getMethod("dropLanguageFunction", ConnectorSession.class, SchemaFunctionName.class, String.class))
                .build());
    }

    private static boolean overrides(Class<?> implementation, Method interfaceMethod)
    {
        Class<?> interfaceClass = interfaceMethod.getDeclaringClass();
        checkArgument(interfaceClass.isInterface());
        checkArgument(!implementation.isInterface());
        checkArgument(interfaceClass.isAssignableFrom(implementation));
        try {
            return implementation.getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes()).getDeclaringClass() != interfaceClass;
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
