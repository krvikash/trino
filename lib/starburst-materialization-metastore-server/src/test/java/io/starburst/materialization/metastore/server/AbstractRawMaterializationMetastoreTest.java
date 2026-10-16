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
package io.starburst.materialization.metastore.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import io.airlift.bootstrap.AutoCloseableCloser;
import io.starburst.materialization.ir.IrVersions;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationDefinition.ConnectorIdVersions;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.StorageTableId;
import io.starburst.materialization.metastore.server.db.ForMaterializationMetastore;
import io.starburst.materialization.metastore.server.db.MaterializationMetastoreFlywayMigration;
import io.starburst.stargate.id.AccountId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import org.flywaydb.core.api.MigrationVersion;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.starburst.materialization.metastore.server.TestingMaterializationMetastoreServer.TESTING_ACCOUNT_RESOLVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractRawMaterializationMetastoreTest
{
    protected static final MetastoreId TEST_METASTORE_ID = new MetastoreId("ms-test");

    private AutoCloseableCloser afterClassCloser;
    /**
     * Jdbi handle to the backing database, used by tests to inspect persisted columns
     * (such as {@code last_modified_at}) that are not exposed through the metastore API.
     */
    private Jdbi jdbi;
    private DbRawMaterializationMetastore dbMaterializationMetastore;
    private TestingMaterializationMetastoreServer server;

    @BeforeAll
    final void setUp()
    {
        afterClassCloser = AutoCloseableCloser.create();
        JdbcDatabaseContainer<?> container = afterClassCloser.register(createDbContainer());
        container.start();
        server = afterClassCloser.register(new TestingMaterializationMetastoreServer(container.getJdbcUrl(), container.getUsername(), container.getPassword(), serverProperties()));
        jdbi = server.getInjector().getInstance(Key.get(Jdbi.class, ForMaterializationMetastore.class));
        dbMaterializationMetastore = server.getInjector().getInstance(DbRawMaterializationMetastore.class);
    }

    @AfterAll
    final void tearDown()
            throws Exception
    {
        afterClassCloser.close();
    }

    protected abstract JdbcDatabaseContainer<?> createDbContainer();

    protected Map<String, String> serverProperties()
    {
        return ImmutableMap.of();
    }

    @BeforeEach
    public void cleanTable()
    {
        jdbi.useHandle(handle -> handle.execute("DELETE FROM materializations"));
    }

    @Test
    public void testCreateListRemove()
    {
        RawMaterializationMetastore metastore = metastore();
        assertThat(metastore.listMaterializations()).isEmpty();

        RawMaterializationDefinition materializationDefinition = definition("catalog", "schema", "mv_a");
        metastore.createOrReplace(materializationDefinition);
        assertThat(metastore.listMaterializations()).containsExactly(materializationDefinition);

        metastore.remove(mvName(materializationDefinition));
        assertThat(metastore.listMaterializations()).isEmpty();
    }

    @Test
    public void testCreateOrReplaceOverwrites()
    {
        RawMaterializationMetastore metastore = metastore();
        RawMaterializationDefinition first = definition("catalog", "schema", "mvName");
        RawMaterializationDefinition second = new RawMaterializationDefinition(
                first.irVersions(),
                first.catalogIrVersions(),
                "{\"plan\":\"updated\"}",
                first.storageTableId(),
                first.source(),
                Instant.ofEpochMilli(1_800_000_000_000L),
                Optional.empty());

        metastore.createOrReplace(first);
        metastore.createOrReplace(second);

        List<RawMaterializationDefinition> all = metastore.listMaterializations();
        assertThat(all).containsExactly(second);
        assertThat(all.get(0).gracePeriod()).isEmpty();
        assertThat(all.get(0).computationPlanRootJson()).isEqualTo("{\"plan\":\"updated\"}");
    }

    @Test
    public void testExpressionIrVersionsRoundTrip()
    {
        RawMaterializationMetastore metastore = metastore();
        RawMaterializationDefinition plain = definition("catalog", "schema", "mv_expressions");
        // Namespaced keys carry ':', '.', '$', '->' and spaces, and every name this build supports is included
        RawMaterializationDefinition withExpressions = new RawMaterializationDefinition(
                IrVersions.SUPPORTED,
                plain.catalogIrVersions(),
                plain.computationPlanRootJson(),
                plain.storageTableId(),
                plain.source(),
                plain.lastKnownFreshTime(),
                plain.gracePeriod());
        assertThat(withExpressions.irVersions()).containsKeys("fn:system.builtin.$operator$equal", "cast:varchar->varchar", "ctype:timestamp with time zone");

        metastore.createOrReplace(withExpressions);

        assertThat(metastore.listMaterializations()).containsExactly(withExpressions);
    }

    @Test
    public void testRemoveMissingIsNoOp()
    {
        RawMaterializationMetastore metastore = metastore();
        metastore.remove(new CatalogSchemaTableName("catalog", "schema", "absent"));
        assertThat(metastore.listMaterializations()).isEmpty();
    }

    @Test
    public void testRenameIfExists()
    {
        RawMaterializationMetastore metastore = metastore();
        RawMaterializationDefinition source = definition("catalog", "schema", "mv_src");
        metastore.createOrReplace(source);
        Instant lastModifiedBeforeRename = lastModifiedAt(mvName(source));

        CatalogSchemaTableName target = new CatalogSchemaTableName("catalog", "schema", "mv_tgt");
        StorageTableId targetStorageTableId = new StorageTableId(
                new CatalogName("storage_catalog"),
                new ConnectorStorageTableId("storage_schema", "mv_tgt_storage", "uid-tgt"));
        metastore.renameIfExists(mvName(source), target, targetStorageTableId);

        List<RawMaterializationDefinition> all = metastore.listMaterializations();
        assertThat(all).hasSize(1);
        RawMaterializationDefinition renamed = all.get(0);
        assertThat(mvName(renamed)).isEqualTo(target);
        assertThat(renamed.storageTableId()).isEqualTo(targetStorageTableId);
        assertThat(renamed.computationPlanRootJson()).isEqualTo(source.computationPlanRootJson());

        assertThat(lastModifiedAt(target)).isAfter(lastModifiedBeforeRename);
    }

    @Test
    public void testRenameMissingIsNoOp()
    {
        RawMaterializationMetastore metastore = metastore();
        metastore.renameIfExists(
                new CatalogSchemaTableName("catalog", "schema", "absent"),
                new CatalogSchemaTableName("catalog", "schema", "target"),
                new StorageTableId(new CatalogName("c"), new ConnectorStorageTableId("s", "t", "u")));
        assertThat(metastore.listMaterializations()).isEmpty();
    }

    @Test
    public void testMetastoresAreIsolated()
    {
        RawMaterializationMetastore tenantA = singleTenant(new MetastoreId("ms-tenant-a"));
        RawMaterializationMetastore tenantB = singleTenant(new MetastoreId("ms-tenant-b"));

        RawMaterializationDefinition inA = definition("catalog", "schema", "mvName");
        tenantA.createOrReplace(inA);
        assertThat(tenantA.listMaterializations()).containsExactly(inA);
        assertThat(tenantB.listMaterializations()).isEmpty();

        // the same name in another tenant is independent
        RawMaterializationDefinition inB = definition("catalog", "schema", "mvName");
        tenantB.createOrReplace(inB);
        tenantA.remove(mvName(inA));
        assertThat(tenantA.listMaterializations()).isEmpty();
        assertThat(tenantB.listMaterializations()).containsExactly(inB);
    }

    @Test
    public void testAccountsAreIsolated()
    {
        RawMaterializationMetastore accountA = singleTenant(dbMaterializationMetastore, TEST_METASTORE_ID, new AccountId("a-10000001"));
        RawMaterializationMetastore accountB = singleTenant(dbMaterializationMetastore, TEST_METASTORE_ID, new AccountId("a-10000002"));

        RawMaterializationDefinition inA = definition("catalog", "schema", "mvName");
        accountA.createOrReplace(inA);
        assertThat(accountB.listMaterializations()).isEmpty();

        accountB.remove(mvName(inA));
        accountB.renameIfExists(mvName(inA), new CatalogSchemaTableName("catalog", "schema", "renamed"), inA.storageTableId());
        assertThat(accountA.listMaterializations()).containsExactly(inA);
    }

    @Test
    public void testUpsertRepairsRowWithoutAccount()
    {
        if (!isAccountIdNullable()) {
            abort("Account id is NOT NULL, so no row lacks it");
        }
        RawMaterializationMetastore metastore = metastore();
        RawMaterializationDefinition definition = definition("catalog", "schema", "mv_null_account");
        metastore.createOrReplace(definition);
        // what a server predating account ids writes
        jdbi.useHandle(handle -> handle.execute("UPDATE materializations SET account_id = NULL"));
        assertThat(metastore.listMaterializations()).isEmpty();

        metastore.createOrReplace(definition);

        assertThat(metastore.listMaterializations()).containsExactly(definition);
    }

    @Test
    public void testUpsertKeepsAccount()
    {
        if (isAccountIdNullable()) {
            abort("Nullable account ids are set on upsert to repair rows written without them");
        }
        RawMaterializationMetastore accountA = singleTenant(dbMaterializationMetastore, TEST_METASTORE_ID, new AccountId("a-10000001"));
        RawMaterializationMetastore accountB = singleTenant(dbMaterializationMetastore, TEST_METASTORE_ID, new AccountId("a-10000002"));
        RawMaterializationDefinition definition = definition("catalog", "schema", "mvName");
        accountA.createOrReplace(definition);

        accountB.createOrReplace(definition);

        assertThat(accountB.listMaterializations()).isEmpty();
        assertThat(accountA.listMaterializations()).containsExactly(definition);
    }

    @Test
    public void testMigrationBackfillsAccountId()
    {
        if (!isAccountIdNullable()) {
            abort("Account id is NOT NULL from the start, so there is nothing to backfill");
        }
        MaterializationMetastoreFlywayMigration migration = server.getInjector().getInstance(MaterializationMetastoreFlywayMigration.class);
        migration.clean();
        migration.migrate(MigrationVersion.fromVersion("1"));
        jdbi.useHandle(handle -> {
            insertV1Row(handle, "ms-backfill-a", "mv_a");
            insertV1Row(handle, "ms-backfill-b", "mv_b");
        });

        migration.migrate(MigrationVersion.LATEST);

        List<String> accountIds = jdbi.withHandle(handle -> handle
                .createQuery("SELECT metastore_id, account_id FROM materializations")
                .map((rs, _) -> rs.getString("metastore_id") + "=" + rs.getString("account_id"))
                .list());
        assertThat(accountIds).containsExactlyInAnyOrder(
                "ms-backfill-a=" + TESTING_ACCOUNT_RESOLVER.accountId(new MetastoreId("ms-backfill-a")),
                "ms-backfill-b=" + TESTING_ACCOUNT_RESOLVER.accountId(new MetastoreId("ms-backfill-b")));
    }

    /**
     * Whether this dialect keeps account id nullable and backfills it from the resolver, which SEP
     * needs for rows written by servers predating account ids.
     */
    protected boolean isAccountIdNullable()
    {
        return true;
    }

    private static void insertV1Row(Handle handle, String metastoreId, String table)
    {
        LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);
        handle.createUpdate(
                        """
                        INSERT INTO materializations (
                            metastore_id, source_catalog, source_schema, source_table,
                            storage_table_catalog, storage_table_schema, storage_table_name, storage_table_unique_id,
                            last_known_fresh_time, ir_versions, catalog_ir_versions, computation_plan_root,
                            created_at, last_modified_at)
                        VALUES (:metastoreId, 'catalog', 'schema', :table, 'storage_catalog', 'storage_schema', :table, 'uid', :now, '{}', '{}', '{}', :now, :now)
                        """)
                .bind("metastoreId", metastoreId)
                .bind("table", table)
                .bind("now", now)
                .execute();
    }

    private RawMaterializationMetastore metastore()
    {
        return singleTenant(TEST_METASTORE_ID);
    }

    protected RawMaterializationMetastore singleTenant(MetastoreId metastoreId)
    {
        return singleTenant(dbMaterializationMetastore, metastoreId);
    }

    protected TestingMaterializationMetastoreServer server()
    {
        return server;
    }

    /**
     * Wraps the multi-tenant DB metastore as a single-tenant {@link RawMaterializationMetastore}
     * bound to {@code metastoreId}, so the shared test cases can exercise the DB implementation.
     */
    protected static RawMaterializationMetastore singleTenant(DbRawMaterializationMetastore metastore, MetastoreId metastoreId)
    {
        return singleTenant(metastore, metastoreId, TESTING_ACCOUNT_RESOLVER.accountId(metastoreId));
    }

    private static RawMaterializationMetastore singleTenant(DbRawMaterializationMetastore metastore, MetastoreId metastoreId, AccountId accountId)
    {
        return new RawMaterializationMetastore()
        {
            @Override
            public List<RawMaterializationDefinition> listMaterializations()
            {
                return metastore.listMaterializations(metastoreId, accountId);
            }

            @Override
            public void createOrReplace(RawMaterializationDefinition materializationDefinition)
            {
                metastore.createOrReplace(metastoreId, accountId, materializationDefinition);
            }

            @Override
            public void remove(CatalogSchemaTableName materializedViewName)
            {
                metastore.remove(metastoreId, accountId, materializedViewName);
            }

            @Override
            public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
            {
                metastore.renameIfExists(metastoreId, accountId, source, target, targetStorageTableId);
            }
        };
    }

    protected static RawMaterializationDefinition definition(String catalog, String schema, String table)
    {
        CatalogSchemaTableName mvName = new CatalogSchemaTableName(catalog, schema, table);
        StorageTableId storageTableId = new StorageTableId(
                new CatalogName("storage_catalog"),
                new ConnectorStorageTableId("storage_schema", truncate(table + "_storage", 256), UUID.randomUUID().toString()));
        return new RawMaterializationDefinition(
                ImmutableMap.of("io.starburst.Output", 3),
                ImmutableMap.of(new CatalogName(catalog), new ConnectorIdVersions(
                        ImmutableSet.of(new ConnectorIdVersion("table", 1)),
                        ImmutableSet.of(new ConnectorIdVersion("column", 2)))),
                "{\"plan\":\"" + table + "\"}",
                storageTableId,
                new MaterializedViewSource(mvName),
                Instant.ofEpochMilli(1_700_000_000_000L),
                Optional.of(Duration.ofMinutes(30)));
    }

    private static String truncate(String string, int maxLength)
    {
        return string.substring(0, Math.min(maxLength, string.length()));
    }

    protected static CatalogSchemaTableName mvName(RawMaterializationDefinition definition)
    {
        return ((MaterializedViewSource) definition.source()).materializedViewName();
    }

    private Instant lastModifiedAt(CatalogSchemaTableName mvName)
    {
        return jdbi.withHandle(handle -> handle
                .createQuery("SELECT last_modified_at FROM materializations WHERE metastore_id = :metastoreId AND source_catalog = :catalog AND source_schema = :schema AND source_table = :table")
                .bind("metastoreId", TEST_METASTORE_ID.id())
                .bind("catalog", mvName.getCatalogName())
                .bind("schema", mvName.getSchemaTableName().getSchemaName())
                .bind("table", mvName.getSchemaTableName().getTableName())
                .map((rs, _) -> rs.getObject("last_modified_at", LocalDateTime.class).toInstant(ZoneOffset.UTC))
                .one());
    }
}
