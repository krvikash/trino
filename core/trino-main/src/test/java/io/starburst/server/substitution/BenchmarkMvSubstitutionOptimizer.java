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
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.ForwardingConnector;
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
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.RemoveRedundantIdentityProjections;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.optimizations.UnaliasSymbolReferences;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.PlanTester;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import io.trino.type.TypeDeserializer;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_ENABLED;
import static io.trino.connector.CatalogServiceProviderModule.createSubstitutionMetadata;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.TransactionManager.DEFAULT_ISOLATION;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Microbenchmark for {@link MvSubstitutionOptimizer#optimize(PlanNode, PlanOptimizer.Context)}. Isolates the
 * optimizer's own CPU cost (the hash tree walk, the per-hash index lookup, the candidate matching loop and the
 * scan rebuild) from connector I/O and planning.
 * <p>
 * State is split by scope so the concurrent benchmark measures real contention rather than a harness artifact:
 * <ul>
 *     <li>{@link BenchmarkData} (benchmark-scoped) holds the shared, read-only fixture - the {@link PlanTester},
 *     the populated {@link MaterializationIndex} and the {@link MvSubstitutionOptimizer} under test.</li>
 *     <li>{@link BenchmarkThreadData} (thread-scoped) holds the per-thread transaction, the pre-substitution
 *     plan and the optimizer {@link PlanOptimizer.Context}, all built once per trial.</li>
 * </ul>
 * Each thread therefore optimizes against its own transaction, matching production (one transaction per query).
 * A single transaction shared across threads would serialize the success path on the {@code synchronized}
 * per-transaction catalog-metadata monitor (every {@code getTableHandle}/{@code getColumnHandles} call), which
 * dominates the measurement and is not a property of the optimizer. The measured region is only
 * {@code optimizer.optimize(plan, context)}; connector metadata handles stay warm within each thread's
 * long-lived transaction, so the numbers reflect the optimizer rather than metadata resolution.
 * <p>
 * The mock {@link ConnectorTableId} ({@link BenchTableId}) decouples the index bucket ({@code hash()})
 * from table identity ({@code name}), so the benchmark can build materializations that collide on hash
 * but fail structural matching, and make exactly one of them match.
 */
@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMvSubstitutionOptimizer
{
    private static final String CATALOG = "mock";
    private static final String SCHEMA = "default";
    private static final ConnectorIdVersion TABLE_ID_VERSION = new ConnectorIdVersion("BenchTableId", 1);
    private static final ConnectorIdVersion COLUMN_ID_VERSION = new ConnectorIdVersion("BenchColumnId", 1);
    private static final String NAME_COLUMN = "name";
    private static final Symbol NAME_SYMBOL = new Symbol(VARCHAR, NAME_COLUMN);
    private static final String STORAGE_TABLE = "storage";
    private static final int WIDE_SCANS = 16;
    // Bucket hashes for non-matching ("filler") materializations. Kept above 2^32 so they can never
    // equal a query scan's hash, which is derived from an int String.hashCode().
    private static final long FILLER_HASH_BASE = 1L << 40;

    public enum PlanShape
    {
        SINGLE_SCAN,
        WIDE_16,
        DEEP_JOIN_16,
    }

    public enum Scenario
    {
        /**
         * {@code mvCount} materializations each in a distinct bucket; the query hash matches none.
         */
        NO_HASH_MATCH,
        /**
         * One matching materialization alone in its bucket, plus {@code mvCount - 1} distinct-hash fillers.
         */
        UNIQUE_MATCH,
        /**
         * {@code mvCount} materializations colliding on the query hash; only the last one matches structurally.
         */
        COLLISION_LAST_MATCHES,
        /**
         * {@code mvCount} materializations colliding on the query hash; none match structurally.
         */
        COLLISION_NONE_MATCH,
        /**
         * One structurally-matching but stale materialization (rejected by the freshness check), plus fillers.
         */
        MATCH_FAILS_FRESHNESS,
        /**
         * {@code mvCount} materializations on the matched table (so {@code sameTable} passes), but exposing a
         * different column id, so matching fails in the per-column mapping loop of the matching visitor.
         */
        MATCH_FAILS_COLUMNS,
    }

    /**
     * Per-thread state: each benchmark thread gets its own transaction, pre-substitution plan and optimizer
     * {@link PlanOptimizer.Context}, built once per trial against the shared {@link BenchmarkData} fixture.
     * Per-thread transactions are deliberate - sharing one across threads would serialize the success path on
     * the {@code synchronized} per-transaction catalog metadata and swamp the measurement (see the class doc).
     */
    @State(Scope.Thread)
    public static class BenchmarkThreadData
    {
        private BenchmarkData benchmarkData;
        private TransactionId transactionId;
        private PlanNode plan;
        private PlanOptimizer.Context context;

        @Setup(Level.Trial)
        public void setup(BenchmarkData benchmarkData)
        {
            this.benchmarkData = benchmarkData;
            transactionId = benchmarkData.transactionManager.beginTransaction(DEFAULT_ISOLATION, false, false);
            // One long-lived transaction for the whole trial; only optimize() runs in the measured region.
            Session session = Session.builder(benchmarkData.planTester.getDefaultSession())
                    .setQueryId(new QueryIdGenerator().createNextQueryId())
                    .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, "true")
                    .build();
            Session transactionSession = session.beginTransactionId(transactionId, benchmarkData.transactionManager, benchmarkData.planTester.getAccessControl());
            benchmarkData.metadata.beginQuery(transactionSession);

            plan = buildInputPlan(transactionSession);
            context = new PlanOptimizer.Context(
                    transactionSession,
                    new SymbolAllocator(),
                    new PlanNodeIdAllocator(),
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector(),
                    new CachingTableStatsProvider(benchmarkData.metadata, transactionSession, () -> false),
                    RuntimeInfoProvider.noImplementation());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            if (benchmarkData.transactionManager != null && transactionId != null) {
                benchmarkData.transactionManager.asyncAbort(transactionId);
            }
        }

        private PlanNode buildInputPlan(Session transactionSession)
        {
            @Language("SQL") String sql = switch (benchmarkData.planShape) {
                case SINGLE_SCAN -> "SELECT name FROM source";
                case WIDE_16 -> IntStream.range(0, WIDE_SCANS)
                        .mapToObj(i -> format("SELECT name FROM source_%s", i))
                        .collect(joining(" UNION ALL "));
                // 16 scans in a left-deep (nested) inner-join tree joined on name; source_0 is the matched leaf
                case DEEP_JOIN_16 -> "SELECT t0.name FROM source_0 t0" +
                        IntStream.range(1, WIDE_SCANS)
                                .mapToObj(i -> format(" JOIN source_%s t%s ON t0.name = t%s.name", i, i, i))
                                .collect(joining());
            };
            Plan inputPlan = benchmarkData.planTester.createPlan(
                    transactionSession,
                    sql,
                    cleanupOptimizers(),
                    ImmutableList.of(),
                    OPTIMIZED,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector());
            return inputPlan.getRoot();
        }

        /**
         * Prune unreferenced columns before substitution, mirroring the optimizer's position in the real
         * pipeline; otherwise the raw scan reads every table column and never matches the materialization.
         */
        private List<PlanOptimizer> cleanupOptimizers()
        {
            return ImmutableList.of(
                    new UnaliasSymbolReferences(),
                    new IterativeOptimizer(
                            "BenchmarkColumnPruning",
                            benchmarkData.planTester.getPlannerContext(),
                            new RuleStatsRecorder(),
                            benchmarkData.planTester.getStatsCalculator(),
                            benchmarkData.planTester.getCostCalculator(),
                            ImmutableSet.<Rule<?>>builder()
                                    .add(new RemoveRedundantIdentityProjections())
                                    .addAll(columnPruningRules(benchmarkData.metadata))
                                    .build()));
        }
    }

    /**
     * Shared, read-only fixture built once per trial: the {@link PlanTester} and its catalog, the
     * {@link MaterializationIndex} populated according to {@link #scenario}, and the
     * {@link MvSubstitutionOptimizer} under test. The {@code @Param} fields enumerate the benchmarked cases.
     */
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"1", "10", "50", "100"})
        private int mvCount;

        @Param({"SINGLE_SCAN", "WIDE_16", "DEEP_JOIN_16"})
        private PlanShape planShape;

        @Param({"NO_HASH_MATCH", "UNIQUE_MATCH", "COLLISION_LAST_MATCHES", "COLLISION_NONE_MATCH", "MATCH_FAILS_FRESHNESS", "MATCH_FAILS_COLUMNS"})
        private Scenario scenario;

        private PlanTester planTester;
        private MvSubstitutionOptimizer optimizer;
        private TransactionManager transactionManager;

        private Metadata metadata;

        @Setup(Level.Trial)
        public void setup()
        {
            planTester = PlanTester.create(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema(SCHEMA)
                    .build());
            planTester.createCatalog(CATALOG, substitutionConnectorFactory(), ImmutableMap.of());
            metadata = planTester.getPlannerContext().getMetadata();

            SubstitutionMetadata substitutionMetadata = new SubstitutionMetadataManager(
                    createSubstitutionMetadata((ConnectorServicesProvider) planTester.getCatalogManager()));
            MaterializationIndex index = buildIndex(substitutionMetadata);
            populateIndex(index);
            optimizer = new MvSubstitutionOptimizer(index, metadata, substitutionMetadata);

            transactionManager = planTester.getTransactionManager();
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            if (planTester != null) {
                planTester.close();
            }
        }

        private MaterializationIndex buildIndex(SubstitutionMetadata substitutionMetadata)
        {
            TypeManager typeManager = planTester.getPlannerContext().getTypeManager();
            JsonCodec<Output> irJsonCodec = new JsonCodecFactory(new JsonMapperProvider()
                    .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager)))
                    .get())
                    .jsonCodec(Output.class);
            return new MaterializationIndex(new VersionAwareMaterializationMetastore(
                    new InMemoryRawMaterializationMetastore(),
                    irJsonCodec,
                    substitutionMetadata,
                    planTester.getCatalogManager()));
        }

        /**
         * The table the matching materialization targets: the only scan in a single-scan plan, otherwise the
         * first scan of a multi-scan plan (a union branch or a join leaf). Its hash is what the
         * collision/match scenarios collide on.
         */
        private String matchedTable()
        {
            return planShape == PlanShape.SINGLE_SCAN ? "source" : "source_0";
        }

        private void populateIndex(MaterializationIndex index)
        {
            long matchedHash = matchedTable().hashCode();
            switch (scenario) {
                case NO_HASH_MATCH -> {
                    for (int i = 0; i < mvCount; i++) {
                        index.createOrReplace(filler(i));
                    }
                }
                case UNIQUE_MATCH -> {
                    index.createOrReplace(materialization("mv_match", matchedTable(), matchedHash, true));
                    for (int i = 0; i < mvCount - 1; i++) {
                        index.createOrReplace(filler(i));
                    }
                }
                case COLLISION_LAST_MATCHES -> {
                    // decoys share the matched hash but a different identity, so they fail sameTable fast
                    for (int i = 0; i < mvCount - 1; i++) {
                        index.createOrReplace(materialization("mv_" + i, "decoy_" + i, matchedHash, true));
                    }
                    index.createOrReplace(materialization("mv_match", matchedTable(), matchedHash, true));
                }
                case COLLISION_NONE_MATCH -> {
                    for (int i = 0; i < mvCount; i++) {
                        index.createOrReplace(materialization("mv_" + i, "decoy_" + i, matchedHash, true));
                    }
                }
                case MATCH_FAILS_FRESHNESS -> {
                    for (int i = 0; i < mvCount; i++) {
                        index.createOrReplace(materialization("mv_match" + i, matchedTable(), matchedHash, false));
                    }
                }
                case MATCH_FAILS_COLUMNS -> {
                    // same table and bucket as the query (sameTable passes), but a different column id, so
                    // matching fails one step later in the per-column mapping loop
                    for (int i = 0; i < mvCount; i++) {
                        index.createOrReplace(materialization("mv_match" + i, matchedTable(), matchedHash, true, "other"));
                    }
                }
            }
        }

        private MaterializationDefinition filler(int i)
        {
            return materialization("mv_filler_" + i, "filler_" + i, FILLER_HASH_BASE + i, true);
        }

        private MaterializationDefinition materialization(String mvName, String sourceTable, long bucketHash, boolean fresh)
        {
            return materialization(mvName, sourceTable, bucketHash, fresh, NAME_COLUMN);
        }

        /**
         * A materialization whose computation is {@code SELECT name FROM <sourceTable>} reading {@code sourceColumn},
         * bucketed under {@code bucketHash}. When {@code fresh} is false it carries an expired grace period so the
         * optimizer rejects it at the freshness check.
         */
        private MaterializationDefinition materialization(String mvName, String sourceTable, long bucketHash, boolean fresh, String sourceColumn)
        {
            Output computation = new Output(
                    ImmutableList.of(NAME_COLUMN),
                    ImmutableList.of(NAME_SYMBOL),
                    new TableScan(
                            new TableId(new CatalogName(CATALOG), new BenchTableId(sourceTable, bucketHash)),
                            ImmutableMap.of(new BenchColumnId(sourceColumn), NAME_SYMBOL)));
            StorageTableId storageTableId = new StorageTableId(
                    new CatalogName(CATALOG),
                    new ConnectorStorageTableId(SCHEMA, STORAGE_TABLE, "v1"));
            return new MaterializationDefinition(
                    computation,
                    storageTableId,
                    new MaterializedViewSource(new CatalogSchemaTableName(CATALOG, SCHEMA, mvName)),
                    fresh ? Instant.now() : Instant.now().minus(Duration.ofDays(1)),
                    fresh ? Optional.empty() : Optional.of(Duration.ofMinutes(5)));
        }
    }

    /**
     * Single-threaded baseline: one {@code optimize} call per invocation against this thread's plan and context.
     */
    @Benchmark
    public PlanNode optimize(BenchmarkData data, BenchmarkThreadData threadData)
    {
        return data.optimizer.optimize(threadData.plan, threadData.context);
    }

    /**
     * Concurrent variant of {@link #optimize}: every thread optimizes its own thread-scoped plan and context,
     * so the only state shared across threads is the read-only {@link MaterializationIndex}. This isolates
     * contention on the optimizer's genuinely shared hot path - chiefly the lock-free {@code getMaterializations}
     * snapshot read - rather than the per-transaction metadata monitor. Runs at {@link Threads#MAX} by default;
     * override with {@code -t} (e.g. {@code -t 1,2,4,8}) for a scaling curve against the single-threaded baseline.
     */
    @Benchmark
    @Threads(Threads.MAX)
    public PlanNode optimizeConcurrent(BenchmarkData data, BenchmarkThreadData threadData)
    {
        return data.optimizer.optimize(threadData.plan, threadData.context);
    }

    @Test
    public void verify()
    {
        for (PlanShape planShape : PlanShape.values()) {
            for (Scenario scenario : Scenario.values()) {
                BenchmarkData data = new BenchmarkData();
                data.mvCount = 10;
                data.planShape = planShape;
                data.scenario = scenario;
                data.setup();
                BenchmarkThreadData threadData = new BenchmarkThreadData();
                threadData.setup(data);
                try {
                    PlanNode optimized = optimize(data, threadData);
                    boolean substituted = storageScanCount(optimized) > 0;
                    boolean expectSubstitution = scenario == Scenario.UNIQUE_MATCH || scenario == Scenario.COLLISION_LAST_MATCHES;
                    assertThat(substituted)
                            .as("substitution for %s / %s", planShape, scenario)
                            .isEqualTo(expectSubstitution);
                }
                finally {
                    data.tearDown();
                }
            }
        }
    }

    private static long storageScanCount(PlanNode node)
    {
        long here = node instanceof TableScanNode scan
                && ((MockConnectorTableHandle) scan.getTable().connectorHandle()).getTableName().getTableName().equals(STORAGE_TABLE)
                ? 1 : 0;
        return here + node.getSources().stream().mapToLong(BenchmarkMvSubstitutionOptimizer::storageScanCount).sum();
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkMvSubstitutionOptimizer.class).run();
    }

    private static ConnectorFactory substitutionConnectorFactory()
    {
        Map<String, List<String>> tableColumns = tableColumns();
        MockConnectorFactory delegate = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of(SCHEMA))
                .withListTables((_, _) -> ImmutableList.copyOf(tableColumns.keySet()))
                .withGetColumns(schemaTableName -> tableColumns.getOrDefault(schemaTableName.getTableName(), ImmutableList.of()).stream()
                        .map(column -> new ColumnMetadata(column, VARCHAR))
                        .collect(toImmutableList()))
                .withGetTableHandle((_, schemaTableName) -> tableColumns.containsKey(schemaTableName.getTableName())
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

    private static Map<String, List<String>> tableColumns()
    {
        ImmutableMap.Builder<String, List<String>> columns = ImmutableMap.builder();
        columns.put("source", ImmutableList.of(NAME_COLUMN));
        columns.put(STORAGE_TABLE, ImmutableList.of(NAME_COLUMN));
        for (int i = 0; i < WIDE_SCANS; i++) {
            columns.put("source_" + i, ImmutableList.of(NAME_COLUMN));
        }
        return columns.buildOrThrow();
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
            return Optional.of(new ConnectorStorageTableId(tableName.getSchemaName(), tableName.getTableName(), "v1"));
        }

        @Override
        public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            String tableName = ((MockConnectorTableHandle) handle).getTableName().getTableName();
            return Optional.of(new BenchTableId(tableName, tableName.hashCode()));
        }

        @Override
        public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
        {
            return Optional.of(new BenchColumnId(((MockConnectorColumnHandle) column).name()));
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

    public record BenchTableId(String name, long hash)
            implements ConnectorTableId
    {
        @Override
        public ConnectorIdVersion version()
        {
            return TABLE_ID_VERSION;
        }
    }

    public record BenchColumnId(String columnName)
            implements ConnectorColumnId
    {
        @Override
        public ConnectorIdVersion version()
        {
            return COLUMN_ID_VERSION;
        }
    }
}
