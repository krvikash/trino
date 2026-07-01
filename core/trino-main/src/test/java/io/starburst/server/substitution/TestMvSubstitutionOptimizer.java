/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.Symbol;
import io.starburst.materialization.ir.TableId;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.InMemoryRawMaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.Session;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.ForwardingConnector;
import io.trino.spi.RefreshType;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewReference;
import io.trino.testing.PlanTester;
import io.trino.type.TypeDeserializer;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_ENABLED;
import static io.trino.connector.CatalogServiceProviderModule.createSubstitutionMetadata;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Plan-level tests for {@link MvSubstitutionOptimizer}. The query scans {@code source}; a
 * materialization records that the same computation is stored in {@code storage}. When substitution
 * is allowed the optimizer rewrites the {@code source} scan into a {@code storage} scan.
 * <p>
 * Substitution runs through the real {@link SubstitutionMetadataManager}, backed by a mock connector
 * (TPCH does not implement {@link ConnectorSubstitutionMetadata}). The connector's identity behaviour
 * is data-driven by table/column name so tests stay independent under concurrent execution:
 * <ul>
 *     <li>{@code unidentified_source} - {@code getTableId} returns empty</li>
 *     <li>{@code unidentified_storage} - {@code getStorageTableId} returns empty</li>
 *     <li>column {@code opaque} - {@code getColumnId} returns empty</li>
 * </ul>
 */
public class TestMvSubstitutionOptimizer
        extends BasePlanTest
{
    private static final String CATALOG = "mock";
    private static final String SCHEMA = "default";
    private static final ConnectorIdVersion TABLE_ID_VERSION = new ConnectorIdVersion("MockTableId", 1);
    private static final ConnectorIdVersion COLUMN_ID_VERSION = new ConnectorIdVersion("MockColumnId", 1);
    private static final Symbol NAME_SYMBOL = new Symbol(VARCHAR, "name");

    private static final Map<String, List<String>> TABLE_COLUMNS = ImmutableMap.of(
            "source", ImmutableList.of("name", "comment"),
            "storage", ImmutableList.of("name", "opaque"),
            "unidentified_source", ImmutableList.of("name"),
            "unidentified_storage", ImmutableList.of("name"),
            "opaque_source", ImmutableList.of("opaque"));

    @Override
    protected PlanTester createPlanTester()
    {
        PlanTester planTester = PlanTester.create(testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build());
        planTester.createCatalog(CATALOG, substitutionConnectorFactory(), ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testSubstitutesScanWithStorageTable()
    {
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
    }

    @Test
    public void testNoSubstitutionWhenDisabled()
    {
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        assertPlan(session(false), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenStale()
    {
        // grace period of 5 minutes, last refreshed a day ago => stale
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofMinutes(5)).lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenStorageTableChanged()
    {
        // the indexed storage id ("v2") no longer matches what the storage table reports today ("v1")
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageUniqueId("v2").build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testSubstitutesWhenFreshWithinGracePeriod()
    {
        // grace period of 1 hour, refreshed just now => fresh
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofHours(1)).build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
    }

    @Test
    public void testNoSubstitutionWhenSourceTableHasNoId()
    {
        // the connector cannot identify the queried table, so no computation hash is produced
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceTable("unidentified_source").build());
        assertPlan(session(true), "SELECT name FROM unidentified_source", optimizer, anyTree(tableScan("unidentified_source")));
    }

    @Test
    public void testNoSubstitutionWhenStorageTableMissing()
    {
        // the indexed storage table no longer resolves to a handle
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageTable("does_not_exist").build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenStorageTableIdAbsent()
    {
        // the storage table can no longer be tagged with a stable id
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageTable("unidentified_storage").build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenStorageColumnMissing()
    {
        // the MV output column has no matching column in the storage table
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().outputColumnName("missing_column").build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenColumnDoesNotMatch()
    {
        // the MV scan exposes a different column than the query references
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceColumnId("other").build());
        assertPlan(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionWhenColumnHasNoId()
    {
        // the connector cannot identify the queried column
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceTable("opaque_source").outputColumnName("opaque").sourceColumnId("opaque").build());
        assertPlan(session(true), "SELECT opaque FROM opaque_source", optimizer, anyTree(tableScan("opaque_source")));
    }

    @Test
    public void testNoSubstitutionWhenColumnIsNotAvailableInMv()
    {
        // the query references a column the MV does not expose
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        assertPlan(session(true), "SELECT comment FROM source", optimizer, anyTree(tableScan("source")));
    }

    @Test
    public void testNoSubstitutionForRefreshMaterializedView()
    {
        // Planning REFRESH must not rewrite the MV's own defining scan to its storage table (a no-op
        // self-read). The source is a real source scan that would otherwise be substituted to storage;
        // wrapping it in a refresh writer must leave the plan untouched (same instance returned).
        PlanTester planTester = getPlanTester();
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        Session session = session(true);
        planTester.inTransaction(session, transactionSession -> {
            PlanNode source = planTester.createPlan(
                    transactionSession,
                    "SELECT name FROM source",
                    cleanupOptimizers(),
                    ImmutableList.of(),
                    OPTIMIZED,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector()).getRoot();
            TableHandle storageTableHandle = planTester.getPlannerContext().getMetadata()
                    .getTableHandle(transactionSession, new QualifiedObjectName(CATALOG, SCHEMA, "storage"))
                    .orElseThrow();
            TableWriterNode refreshPlan = new TableWriterNode(
                    new PlanNodeId("refresh"),
                    source,
                    new RefreshMaterializedViewReference(
                            "test_mv",
                            new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv"),
                            storageTableHandle,
                            ImmutableList.of(),
                            ImmutableList.of(),
                            false,
                            RefreshType.FULL),
                    new io.trino.sql.planner.Symbol(BIGINT, "rows"),
                    new io.trino.sql.planner.Symbol(VARBINARY, "fragment"),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            assertThat(optimizer.optimize(refreshPlan, context(transactionSession))).isSameAs(refreshPlan);
            return null;
        });
    }

    private static MaterializationBuilder materialization()
    {
        return new MaterializationBuilder();
    }

    private static final class MaterializationBuilder
    {
        private String sourceTable = "source";
        private String storageTable = "storage";
        private String storageUniqueId = "v1";
        private Optional<Duration> gracePeriod = Optional.empty();
        private Instant lastKnownFreshTime = Instant.now();
        private String outputColumnName = "name";
        private String sourceColumnId = "name";

        MaterializationBuilder sourceTable(String sourceTable)
        {
            this.sourceTable = sourceTable;
            return this;
        }

        MaterializationBuilder storageTable(String storageTable)
        {
            this.storageTable = storageTable;
            return this;
        }

        MaterializationBuilder storageUniqueId(String storageUniqueId)
        {
            this.storageUniqueId = storageUniqueId;
            return this;
        }

        MaterializationBuilder gracePeriod(Duration gracePeriod)
        {
            this.gracePeriod = Optional.of(gracePeriod);
            return this;
        }

        MaterializationBuilder lastKnownFreshTime(Instant lastKnownFreshTime)
        {
            this.lastKnownFreshTime = lastKnownFreshTime;
            return this;
        }

        MaterializationBuilder outputColumnName(String outputColumnName)
        {
            this.outputColumnName = outputColumnName;
            return this;
        }

        MaterializationBuilder sourceColumnId(String sourceColumnId)
        {
            this.sourceColumnId = sourceColumnId;
            return this;
        }

        MaterializationDefinition build()
        {
            Output computation = new Output(
                    ImmutableList.of(outputColumnName),
                    ImmutableList.of(NAME_SYMBOL),
                    new TableScan(
                            new TableId(new CatalogName(CATALOG), new MockTableId(sourceTable)),
                            ImmutableMap.of(new MockColumnId(sourceColumnId), NAME_SYMBOL)));
            StorageTableId storageTableId = new StorageTableId(
                    new CatalogName(CATALOG),
                    new ConnectorStorageTableId(SCHEMA, storageTable, storageUniqueId));
            return new MaterializationDefinition(
                    computation,
                    storageTableId,
                    new MaterializedViewSource(new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv")),
                    lastKnownFreshTime,
                    gracePeriod);
        }
    }

    private MvSubstitutionOptimizer optimizerWith(MaterializationDefinition materialization)
    {
        PlanTester planTester = getPlanTester();
        SubstitutionMetadata substitutionMetadata = new SubstitutionMetadataManager(
                createSubstitutionMetadata((ConnectorServicesProvider) planTester.getCatalogManager()));
        TypeManager typeManager = planTester.getPlannerContext().getTypeManager();
        JsonCodec<Output> irJsonCodec = new JsonCodecFactory(new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager)))
                .get())
                .jsonCodec(Output.class);
        MaterializationIndex index = new MaterializationIndex(new VersionAwareMaterializationMetastore(
                new InMemoryRawMaterializationMetastore(),
                irJsonCodec,
                substitutionMetadata,
                planTester.getCatalogManager()));
        index.createOrReplace(materialization);
        return new MvSubstitutionOptimizer(index, planTester.getPlannerContext().getMetadata(), substitutionMetadata, planTester.getAccessControl());
    }

    private Session session(boolean substitutionEnabled)
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, Boolean.toString(substitutionEnabled))
                .build();
    }

    private void assertPlan(Session session, @Language("SQL") String sql, PlanOptimizer optimizer, PlanMatchPattern pattern)
    {
        PlanTester planTester = getPlanTester();
        List<PlanOptimizer> optimizers = ImmutableList.<PlanOptimizer>builder()
                .addAll(cleanupOptimizers())
                .add(optimizer)
                .build();
        planTester.inTransaction(session, transactionSession -> {
            Plan actualPlan = planTester.createPlan(
                    transactionSession,
                    sql,
                    optimizers,
                    ImmutableList.of(),
                    OPTIMIZED,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector());
            PlanAssert.assertPlan(
                    transactionSession,
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    planTester.getStatsCalculator(),
                    actualPlan,
                    pattern);
            return null;
        });
    }

    /**
     * Prune unreferenced columns before substitution, mirroring the optimizer's position in the
     * real pipeline; otherwise the raw scan reads every table column and never matches the MV.
     */
    private List<PlanOptimizer> cleanupOptimizers()
    {
        PlanTester planTester = getPlanTester();
        Metadata metadata = planTester.getPlannerContext().getMetadata();
        return ImmutableList.of(
                new UnaliasSymbolReferences(),
                new IterativeOptimizer(
                        "TestColumnPruning",
                        planTester.getPlannerContext(),
                        new RuleStatsRecorder(),
                        planTester.getStatsCalculator(),
                        planTester.getCostCalculator(),
                        ImmutableSet.<Rule<?>>builder()
                                .add(new RemoveRedundantIdentityProjections())
                                .addAll(columnPruningRules(metadata))
                                .build()));
    }

    private PlanOptimizer.Context context(Session session)
    {
        Metadata metadata = getPlanTester().getPlannerContext().getMetadata();
        return new PlanOptimizer.Context(
                session,
                new SymbolAllocator(),
                new PlanNodeIdAllocator(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new CachingTableStatsProvider(metadata, session, () -> false),
                RuntimeInfoProvider.noImplementation());
    }

    private static ConnectorFactory substitutionConnectorFactory()
    {
        MockConnectorFactory delegate = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of(SCHEMA))
                .withListTables((_, _) -> ImmutableList.copyOf(TABLE_COLUMNS.keySet()))
                .withGetColumns(schemaTableName -> TABLE_COLUMNS.getOrDefault(schemaTableName.getTableName(), ImmutableList.of()).stream()
                        .map(column -> new ColumnMetadata(column, VARCHAR))
                        .collect(toImmutableList()))
                .withGetTableHandle((_, schemaTableName) -> TABLE_COLUMNS.containsKey(schemaTableName.getTableName())
                        ? new MockConnectorTableHandle(schemaTableName)
                        : null)
                .build();
        return new ConnectorFactory()
        {
            @Override
            public String getName()
            {
                return "mock_substitution";
            }

            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                Connector connector = delegate.create(catalogName, config, context);
                return new ForwardingConnector()
                {
                    @Override
                    protected Connector delegate()
                    {
                        return connector;
                    }

                    @Override
                    public ConnectorSubstitutionMetadata getSubstitutionMetadata()
                    {
                        return new MockSubstitutionMetadata();
                    }
                };
            }
        };
    }

    private static class MockSubstitutionMetadata
            implements ConnectorSubstitutionMetadata
    {
        @Override
        public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
        {
            return getTableId(session, queryTable)
                    .map(candidateTable::equals)
                    .orElse(false);
        }

        @Override
        public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            SchemaTableName tableName = ((MockConnectorTableHandle) handle).getTableName();
            if (tableName.getTableName().equals("unidentified_storage")) {
                return Optional.empty();
            }
            return Optional.of(new ConnectorStorageTableId(tableName.getSchemaName(), tableName.getTableName(), "v1"));
        }

        @Override
        public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            SchemaTableName tableName = ((MockConnectorTableHandle) handle).getTableName();
            if (tableName.getTableName().equals("unidentified_source")) {
                return Optional.empty();
            }
            return Optional.of(new MockTableId(tableName.getTableName()));
        }

        @Override
        public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
        {
            String columnName = ((MockConnectorColumnHandle) column).name();
            if (columnName.equals("opaque")) {
                return Optional.empty();
            }
            return Optional.of(new MockColumnId(columnName));
        }

        @Override
        public Set<ConnectorIdVersion> tableIdVersions()
        {
            return ImmutableSet.of(TABLE_ID_VERSION);
        }

        @Override
        public Set<ConnectorIdVersion> columnIdVersions()
        {
            return ImmutableSet.of(COLUMN_ID_VERSION);
        }
    }

    public record MockTableId(String tableName)
            implements ConnectorTableId
    {
        @Override
        public long hash()
        {
            return tableName.hashCode();
        }

        @Override
        public ConnectorIdVersion version()
        {
            return TABLE_ID_VERSION;
        }
    }

    public record MockColumnId(String columnName)
            implements ConnectorColumnId
    {
        @Override
        public ConnectorIdVersion version()
        {
            return COLUMN_ID_VERSION;
        }
    }
}
