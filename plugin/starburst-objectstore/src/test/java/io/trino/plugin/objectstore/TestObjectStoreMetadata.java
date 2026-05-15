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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.airlift.slice.Slices;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hudi.HudiMetadata;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThatNoException;

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

    @Test // regression test for https://starburstdata.atlassian.net/browse/ENG-15282
    public void testGetIcebergTableHandle(@TempDir Path tempDir)
    {
        String schemaName = "default";
        ObjectStoreConnector firstConnector = createConnector(tempDir);

        ConnectorTransactionHandle firstTransaction = firstConnector.beginTransaction(READ_UNCOMMITTED, false, true);
        ConnectorMetadata metadata = firstConnector.getMetadata(SESSION, firstTransaction);
        metadata.createSchema(SESSION, schemaName, Map.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        firstConnector.commit(firstTransaction);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new ObjectStoreSessionProperties(
                        firstConnector.getDelegates(),
                        new FeatureExposures(Optional.empty(), Optional.empty(), Optional.empty()))
                        .getSessionProperties())
                .build();

        // Create a new Iceberg table
        createIcebergTable(session, firstConnector, schemaName, "test_iceberg");

        // Create two Hive tables to make 'HIVE' rank higher in RelationTypeCache
        ObjectStoreConnector secondConnector = createConnector(tempDir);
        ConnectorTransactionHandle secondTransaction = secondConnector.beginTransaction(READ_UNCOMMITTED, false, true);
        ConnectorMetadata secondMetadata = secondConnector.getMetadata(session, secondTransaction);

        createHiveTable(session, secondConnector, schemaName, "test_hive");
        createHiveTable(session, secondConnector, schemaName, "test_hive2");

        assertThatNoException().isThrownBy(() -> secondMetadata.getTableHandle(
                session,
                new SchemaTableName(schemaName, "test_iceberg"),
                Optional.empty(),
                Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, VarcharType.VARCHAR, Slices.utf8Slice("main")))));
    }

    private static ObjectStoreConnector createConnector(Path tempDir)
    {
        ConnectorFactory connectorFactory = getOnlyElement(Iterators.filter(new ObjectStorePlugin().getConnectorFactories().iterator(), factory -> factory.getName().equals(STARBURST_OBJECTSTORE)));
        return (ObjectStoreConnector) connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", tempDir.toString())
                        .put("fs.hadoop.enabled", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
    }

    private static void createIcebergTable(ConnectorSession session, ObjectStoreConnector connector, String schemaName, String tableName)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, false, true);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                List.of(new ColumnMetadata("x", IntegerType.INTEGER)),
                Map.of("type", TableType.ICEBERG, FORMAT_VERSION_PROPERTY, 2, FILE_FORMAT_PROPERTY, IcebergFileFormat.PARQUET.name()));
        ConnectorMetadata metadata = connector.getMetadata(session, transaction);
        metadata.createTable(session, tableMetadata, SaveMode.FAIL);
    }

    private static void createHiveTable(ConnectorSession session, ObjectStoreConnector connector, String schemaName, String tableName)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, false, true);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                List.of(new ColumnMetadata("x", IntegerType.INTEGER)),
                Map.of("type", TableType.HIVE, BUCKET_COUNT_PROPERTY, 0, BUCKETED_BY_PROPERTY, List.of(), SORTED_BY_PROPERTY, List.of(), STORAGE_FORMAT_PROPERTY, "TEXTFILE"));
        ConnectorMetadata metadata = connector.getMetadata(session, transaction);
        metadata.createTable(session, tableMetadata, SaveMode.FAIL);
    }
}
