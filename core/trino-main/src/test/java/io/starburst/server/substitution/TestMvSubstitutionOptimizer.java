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
import io.airlift.stats.CounterStat;
import io.opentelemetry.api.metrics.MeterProvider;
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
import io.trino.cost.StatsAndCosts;
import io.trino.execution.TableInfo;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.ForwardingConnector;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.security.SecurityContext;
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
import io.trino.spi.eventlistener.MaterializedViewSubstitutionOutcome;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.RuleStatsRecorder;
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
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.RefreshMaterializedViewReference;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.PlanTester;
import io.trino.type.TypeDeserializer;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.weakref.jmx.Managed;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.materialization.ir.IrJsonMapper.irJsonMapper;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATES_REGEX_FILTER;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_ENABLED;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_MAX_STALENESS;
import static io.trino.connector.CatalogServiceProviderModule.createSubstitutionMetadata;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.PlanOptimizers.columnPruningRules;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
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
            "storage2", ImmutableList.of("name", "opaque"),
            "unidentified_source", ImmutableList.of("name"),
            "unidentified_storage", ImmutableList.of("name"),
            "opaque_source", ImmutableList.of("opaque"),
            "coercion_source", ImmutableList.of("name"));

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
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
        assertThat(substitutions.get(0).materializedView()).contains(new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv"));
    }

    @Test
    public void testSubstitutedRowCarriesReplacedPlan()
    {
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        MaterializedViewSubstitution substituted = substitutions.get(0);
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);

        MaterializedViewSubstitution.ReplacedPlan replacedPlan = substituted.replacedPlan().orElseThrow();
        assertThat(replacedPlan.root()).isInstanceOf(TableScanNode.class);
        TableScanNode replacedScan = (TableScanNode) replacedPlan.root();
        TableInfo tableInfo = replacedPlan.tableInfos().get(replacedScan.getId());
        assertThat(tableInfo).isNotNull();
        assertThat(tableInfo.tableName()).isEqualTo(new QualifiedObjectName(CATALOG, SCHEMA, "source"));
    }

    @Test
    public void testRejectedRowHasNoReplacedPlan()
    {
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofMinutes(5)).lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STALE);
        assertThat(substitutions.get(0).replacedPlan()).isEmpty();
    }

    @Test
    public void testNoSubstitutionWhenDisabled()
    {
        // the global disabled gate is not a near-miss and is not recorded
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(false), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertThat(substitutions).isEmpty();
    }

    @Test
    public void testNoSubstitutionWhenStale()
    {
        // grace period of 5 minutes, last refreshed a day ago => stale
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofMinutes(5)).lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STALE);
    }

    @Test
    public void testNoSubstitutionWhenStorageTableChanged()
    {
        // the indexed storage id ("v2") no longer matches what the storage table reports today ("v1")
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageUniqueId("v2").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STORAGE_TABLE_CHANGED);
    }

    @Test
    public void testSubstitutesWhenFreshWithinGracePeriod()
    {
        // grace period of 1 hour, refreshed just now => fresh
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofHours(1)).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
    }

    @Test
    public void testNoSubstitutionWhenExceedingMaxStaleness()
    {
        // within the MV's 1-day grace period, but refreshed 30 minutes ago while the session max staleness is 5 minutes => rejected
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().gracePeriod(Duration.ofDays(1)).lastKnownFreshTime(Instant.now().minus(Duration.ofMinutes(30))).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithMaxStaleness("5m"), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.MAX_STALENESS_EXCEEDED);
    }

    @Test
    public void testMaxStalenessBoundsUnlimitedGracePeriod()
    {
        // unlimited grace period, refreshed a day ago, but the session max staleness is 1 hour => rejected
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithMaxStaleness("1h"), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.MAX_STALENESS_EXCEEDED);
    }

    @Test
    public void testSubstitutesWithinMaxStaleness()
    {
        // unlimited grace period, refreshed just now, session max staleness of 1 hour => fresh
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithMaxStaleness("1h"), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
    }

    @Test
    public void testSubstitutesWhenNameMatchesPattern()
    {
        // the MV is mock.default.test_mv and the pattern matches its fully qualified name
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithCandidatesRegexFilter("mock\\.default\\..*"), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
    }

    @Test
    public void testNoSubstitutionWhenNameDoesNotMatchPattern()
    {
        // the pattern targets a different catalog, so mock.default.test_mv is not eligible
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithCandidatesRegexFilter("other_catalog\\..*"), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.EXCLUDED_BY_FILTER);
    }

    @Test
    public void testNoSubstitutionWhenPatternMatchesOnlyPartOfName()
    {
        // full-match semantics: a pattern matching just the catalog does not match the whole name
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithCandidatesRegexFilter("mock"), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.EXCLUDED_BY_FILTER);
    }

    @Test
    public void testSubstitutesWhenPatternMatchesAllNames()
    {
        // a catch-all pattern makes every materialized view eligible
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithCandidatesRegexFilter(".*"), "SELECT name FROM source", optimizer, anyTree(tableScan("storage")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
    }

    @Test
    public void testNoSubstitutionWhenPatternMatchesNoNames()
    {
        // a pattern that can never match excludes every materialized view
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(sessionWithCandidatesRegexFilter("(?!)"), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.EXCLUDED_BY_FILTER);
    }

    @Test
    public void testNoSubstitutionWhenSourceTableHasNoId()
    {
        // the connector cannot identify the queried table, so no computation hash is produced and nothing is recorded
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceTable("unidentified_source").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM unidentified_source", optimizer, anyTree(tableScan("unidentified_source")));
        assertThat(substitutions).isEmpty();
    }

    @Test
    public void testNoSubstitutionWhenStorageTableMissing()
    {
        // the indexed storage table no longer resolves to a handle
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageTable("does_not_exist").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STORAGE_TABLE_NOT_FOUND);
    }

    @Test
    public void testNoSubstitutionWhenStorageTableIdAbsent()
    {
        // the storage table can no longer be tagged with a stable id
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().storageTable("unidentified_storage").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STORAGE_TABLE_CHANGED);
    }

    @Test
    public void testNoSubstitutionWhenStorageColumnMissing()
    {
        // the MV output column has no matching column in the storage table
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().outputColumnName("missing_column").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.STORAGE_COLUMN_MISSING);
    }

    @Test
    public void testNoSubstitutionWhenColumnDoesNotMatch()
    {
        // the MV scan exposes a different column than the query references
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceColumnId("other").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.COLUMN_NOT_IN_MATERIALIZED_VIEW);
    }

    @Test
    public void testNoSubstitutionWhenColumnHasNoId()
    {
        // the connector cannot identify the queried column
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceTable("opaque_source").outputColumnName("opaque").sourceColumnId("opaque").build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT opaque FROM opaque_source", optimizer, anyTree(tableScan("opaque_source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.COMPUTATION_MISMATCH);
    }

    @Test
    public void testNoSubstitutionWhenColumnIsNotAvailableInMv()
    {
        // the query references a column the MV does not expose
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT comment FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.COLUMN_NOT_IN_MATERIALIZED_VIEW);
    }

    @Test
    public void testSubstitutesWithStorageTypeCoercion()
    {
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().sourceTable("coercion_source")
                .nameSymbol(new Symbol(createVarcharType(10), "name")).build());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(
                session(true),
                "SELECT name FROM coercion_source",
                optimizer,
                anyTree(project(
                        ImmutableMap.of("coerced", expression(new Cast(new Reference(VARCHAR, "storage_name"), createVarcharType(10)))),
                        tableScan("storage", ImmutableMap.of("storage_name", "name")))));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
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

            MvSubstitutionCollector collector = new MvSubstitutionCollector();
            assertThat(optimizer.optimize(refreshPlan, context(transactionSession, collector))).isSameAs(refreshPlan);
            // the refresh-plan gate is not a near-miss and is not recorded
            assertThat(collector.getSubstitutions()).isEmpty();
            return null;
        });
    }

    @Test
    public void testAccessDeniedRedactsMaterializedViewName()
    {
        // the MV name is redacted from the recorded row because naming it would disclose an object
        // the user was just denied access to
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build(), new DenySelectOnMaterializedViewAccessControl());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.ACCESS_DENIED);
        assertThat(substitutions.get(0).materializedView()).isEmpty();
    }

    @Test
    public void testAccessDeniedTakesPrecedenceOverOtherRejectionReasons()
    {
        // a candidate that is both stale and inaccessible is recorded as ACCESS_DENIED, without its name
        MvSubstitutionOptimizer optimizer = optimizerWith(
                materialization().gracePeriod(Duration.ofMinutes(5)).lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build(),
                new DenySelectOnMaterializedViewAccessControl());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.ACCESS_DENIED);
        assertThat(substitutions.get(0).materializedView()).isEmpty();
    }

    @Test
    public void testColumnAccessDeniedRecordsMaterializedViewName()
    {
        // the user can see the MV (table-level check passes) but lacks SELECT on the column it would read
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build(), new DenySelectOnMaterializedViewColumnsAccessControl());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("source")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.COLUMN_ACCESS_DENIED);
        assertThat(substitutions.get(0).materializedView()).contains(new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv"));
    }

    @Test
    public void testNonSubstitutableScanIsRecordedOnlyWhenACandidateCoversIt()
    {
        PlanTester planTester = getPlanTester();
        MvSubstitutionOptimizer optimizer = optimizerWith(materialization().build());
        Session session = session(true);
        planTester.inTransaction(session, transactionSession -> {
            // the materialization covers this computation, so a scan that cannot be substituted is a near miss
            MvSubstitutionCollector covered = new MvSubstitutionCollector();
            optimizer.optimize(updateTargetScan(transactionSession, "source"), context(transactionSession, covered));
            assertOutcomes(covered.getSubstitutions(), MaterializedViewSubstitutionOutcome.SCAN_NOT_SUBSTITUTABLE);

            // no materialization covers this computation, so there is nothing to record
            MvSubstitutionCollector uncovered = new MvSubstitutionCollector();
            optimizer.optimize(updateTargetScan(transactionSession, "storage"), context(transactionSession, uncovered));
            assertThat(uncovered.getSubstitutions()).isEmpty();
            return null;
        });
    }

    @Test
    public void testFallsBackToSecondCandidateWhenFirstIsAccessDenied()
    {
        // two MVs materialize the same computation; the first is access-denied so the optimizer
        // falls back to the second candidate rather than giving up on the base table
        MvSubstitutionOptimizer optimizer = optimizerWith(
                ImmutableList.of(
                        materialization().build(),
                        materialization().materializedViewName("test_mv_2").storageTable("storage2").build()),
                new DenySelectOnMaterializedViewAccessControl());
        List<MaterializedViewSubstitution> substitutions = assertPlanAndSubstitutions(session(true), "SELECT name FROM source", optimizer, anyTree(tableScan("storage2")));
        assertOutcomes(substitutions, MaterializedViewSubstitutionOutcome.ACCESS_DENIED, MaterializedViewSubstitutionOutcome.SUBSTITUTED);
        assertThat(substitutions.get(0).materializedView()).isEmpty();
        assertThat(substitutions.get(1).materializedView()).contains(new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv_2"));
    }

    @Test
    public void testRecordedSubstitutionsAreCapped()
    {
        // MAX_RECORDED_ROWS_PER_KIND caps rejections per kind, so a query with many candidate MVs cannot grow the payload unbounded
        MvSubstitutionCollector collector = new MvSubstitutionCollector();
        for (int i = 0; i < 150; i++) {
            collector.recordRejected(Optional.empty(), MaterializedViewSubstitutionOutcome.SCAN_NOT_SUBSTITUTABLE);
        }
        assertThat(collector.getSubstitutions()).hasSize(100);
    }

    @Test
    public void testSubstitutionIsRecordedAfterTheRejectionBudgetIsExhausted()
    {
        MvSubstitutionCollector collector = new MvSubstitutionCollector();
        for (int i = 0; i < 150; i++) {
            collector.recordRejected(Optional.empty(), MaterializedViewSubstitutionOutcome.SCAN_NOT_SUBSTITUTABLE);
        }
        collector.recordSubstituted(
                new CatalogSchemaTableName(CATALOG, SCHEMA, "test_mv"),
                new CatalogSchemaTableName(CATALOG, SCHEMA, "storage"),
                Instant.now(),
                new MaterializedViewSubstitution.ReplacedPlan(new ValuesNode(new PlanNodeId("scan-substituted"), 0), ImmutableMap.of()));
        assertThat(collector.getSubstitutions())
                .filteredOn(info -> info.outcome() == MaterializedViewSubstitutionOutcome.SUBSTITUTED)
                .hasSize(1);
    }

    @Test
    public void testEveryOutcomeHasManagedCounter()
            throws Exception
    {
        MvSubstitutionStats stats = new MvSubstitutionStats();
        for (MaterializedViewSubstitutionOutcome outcome : MaterializedViewSubstitutionOutcome.values()) {
            Method getter = MvSubstitutionStats.class.getMethod("get" + UPPER_UNDERSCORE.to(UPPER_CAMEL, outcome.name()));
            assertThat(getter.isAnnotationPresent(Managed.class)).isTrue();
            CounterStat counter = (CounterStat) getter.invoke(stats);
            stats.increment(outcome);
            assertThat(counter.getTotalCount()).isEqualTo(1);
        }
    }

    @Test
    public void testStatsRecordSubstitutionOutcomes()
    {
        // MvSubstitutionStats exposes live per-outcome counters via JMX; assert they actually increment
        MvSubstitutionStats stats = new MvSubstitutionStats();
        MvSubstitutionOptimizer substituting = optimizerWith(ImmutableList.of(materialization().build()), getPlanTester().getAccessControl(), stats);
        assertPlanAndSubstitutions(session(true), "SELECT name FROM source", substituting, anyTree(tableScan("storage")));
        assertThat(stats.getSubstituted().getTotalCount()).isEqualTo(1);
        assertThat(stats.getStale().getTotalCount()).isEqualTo(0);

        MvSubstitutionOptimizer rejecting = optimizerWith(
                ImmutableList.of(materialization().gracePeriod(Duration.ofMinutes(5)).lastKnownFreshTime(Instant.now().minus(Duration.ofDays(1))).build()),
                getPlanTester().getAccessControl(),
                stats);
        assertPlanAndSubstitutions(session(true), "SELECT name FROM source", rejecting, anyTree(tableScan("source")));
        assertThat(stats.getStale().getTotalCount()).isEqualTo(1);
        assertThat(stats.getSubstituted().getTotalCount()).isEqualTo(1);
    }

    private static class DenySelectOnMaterializedViewAccessControl
            extends AllowAllAccessControl
    {
        @Override
        public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Optional<String> branch, Set<String> columnNames)
        {
            if (tableName.objectName().equals("test_mv")) {
                throw new AccessDeniedException("Cannot select from " + tableName);
            }
        }
    }

    private static class DenySelectOnMaterializedViewColumnsAccessControl
            extends AllowAllAccessControl
    {
        @Override
        public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Optional<String> branch, Set<String> columnNames)
        {
            // the table-level check (called with an empty column set) passes; only a non-empty,
            // column-specific check is denied
            if (tableName.objectName().equals("test_mv") && !columnNames.isEmpty()) {
                throw new AccessDeniedException("Cannot select columns " + columnNames + " from " + tableName);
            }
        }
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
        private Symbol nameSymbol = NAME_SYMBOL;
        private String materializedViewName = "test_mv";

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

        MaterializationBuilder nameSymbol(Symbol nameSymbol)
        {
            this.nameSymbol = nameSymbol;
            return this;
        }

        MaterializationBuilder materializedViewName(String materializedViewName)
        {
            this.materializedViewName = materializedViewName;
            return this;
        }

        MaterializationDefinition build()
        {
            Output computation = new Output(
                    ImmutableList.of(outputColumnName),
                    ImmutableList.of(nameSymbol),
                    new TableScan(
                            new TableId(new CatalogName(CATALOG), new MockTableId(sourceTable)),
                            ImmutableMap.of(new MockColumnId(sourceColumnId), nameSymbol)));
            StorageTableId storageTableId = new StorageTableId(
                    new CatalogName(CATALOG),
                    new ConnectorStorageTableId(SCHEMA, storageTable, storageUniqueId));
            return new MaterializationDefinition(
                    computation,
                    storageTableId,
                    new MaterializedViewSource(new CatalogSchemaTableName(CATALOG, SCHEMA, materializedViewName)),
                    lastKnownFreshTime,
                    gracePeriod);
        }
    }

    private MvSubstitutionOptimizer optimizerWith(MaterializationDefinition materialization)
    {
        return optimizerWith(ImmutableList.of(materialization), getPlanTester().getAccessControl());
    }

    private MvSubstitutionOptimizer optimizerWith(MaterializationDefinition materialization, AccessControl accessControl)
    {
        return optimizerWith(ImmutableList.of(materialization), accessControl);
    }

    private MvSubstitutionOptimizer optimizerWith(List<MaterializationDefinition> materializations, AccessControl accessControl)
    {
        return optimizerWith(materializations, accessControl, new MvSubstitutionStats());
    }

    private MvSubstitutionOptimizer optimizerWith(List<MaterializationDefinition> materializations, AccessControl accessControl, MvSubstitutionStats stats)
    {
        PlanTester planTester = getPlanTester();
        SubstitutionMetadata substitutionMetadata = new SubstitutionMetadataManager(
                createSubstitutionMetadata((ConnectorServicesProvider) planTester.getCatalogManager()));
        TypeManager typeManager = planTester.getPlannerContext().getTypeManager();
        JsonCodec<Output> irJsonCodec = new JsonCodecFactory(irJsonMapper(new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager)))
                .get()))
                .jsonCodec(Output.class);
        MaterializationIndex index = new MaterializationIndex(new VersionAwareMaterializationMetastore(
                new InMemoryRawMaterializationMetastore(),
                irJsonCodec),
                new MaterializedViewSubstitutionConfig());
        materializations.forEach(index::createOrReplace);
        return new MvSubstitutionOptimizer(
                index,
                planTester.getPlannerContext().getMetadata(),
                substitutionMetadata,
                planTester.getPlannerContext().getTypeManager(),
                accessControl,
                stats,
                new MvSubstitutionUsageMetrics(MeterProvider.noop().get("noop")));
    }

    private TableScanNode updateTargetScan(Session session, String tableName)
    {
        Metadata metadata = getPlanTester().getPlannerContext().getMetadata();
        TableHandle table = metadata.getTableHandle(session, new QualifiedObjectName(CATALOG, SCHEMA, tableName)).orElseThrow();
        ColumnHandle nameColumn = metadata.getColumnHandles(session, table).get("name");
        io.trino.sql.planner.Symbol name = new io.trino.sql.planner.Symbol(VARCHAR, "name");
        return new TableScanNode(
                new PlanNodeId("scan"),
                table,
                ImmutableList.of(name),
                ImmutableMap.of(name, nameColumn),
                TupleDomain.all(),
                Optional.empty(),
                true,
                Optional.empty());
    }

    private static void assertOutcomes(List<MaterializedViewSubstitution> substitutions, MaterializedViewSubstitutionOutcome... expected)
    {
        assertThat(substitutions.stream().map(MaterializedViewSubstitution::outcome))
                .containsExactly(expected);
    }

    private Session session(boolean substitutionEnabled)
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, Boolean.toString(substitutionEnabled))
                .build();
    }

    private Session sessionWithMaxStaleness(String maxStaleness)
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, Boolean.toString(true))
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_MAX_STALENESS, maxStaleness)
                .build();
    }

    private Session sessionWithCandidatesRegexFilter(String candidatesRegexFilter)
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, Boolean.toString(true))
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATES_REGEX_FILTER, candidatesRegexFilter)
                .build();
    }

    /**
     * Plans {@code sql} with the cleanup optimizers, then runs {@code optimizer} directly against a
     * fresh {@link MvSubstitutionCollector} (rather than folding it into the {@code PlanTester}-driven
     * optimizer list, which always plans against the shared no-op collector) so the recorded outcomes
     * can be asserted on.
     */
    private List<MaterializedViewSubstitution> assertPlanAndSubstitutions(Session session, @Language("SQL") String sql, MvSubstitutionOptimizer optimizer, PlanMatchPattern pattern)
    {
        PlanTester planTester = getPlanTester();
        MvSubstitutionCollector collector = new MvSubstitutionCollector();
        planTester.inTransaction(session, transactionSession -> {
            PlanNode initialPlan = planTester.createPlan(
                    transactionSession,
                    sql,
                    cleanupOptimizers(),
                    ImmutableList.of(),
                    OPTIMIZED,
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector()).getRoot();
            PlanNode optimizedRoot = optimizer.optimize(initialPlan, context(transactionSession, collector));
            Plan actualPlan = new Plan(optimizedRoot, StatsAndCosts.empty());
            PlanAssert.assertPlan(
                    transactionSession,
                    planTester.getPlannerContext().getMetadata(),
                    planTester.getPlannerContext().getFunctionManager(),
                    planTester.getStatsCalculator(),
                    actualPlan,
                    pattern);
            return null;
        });
        return collector.getSubstitutions();
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
        return context(session, new MvSubstitutionCollector());
    }

    private PlanOptimizer.Context context(Session session, MvSubstitutionCollector mvSubstitutionCollector)
    {
        Metadata metadata = getPlanTester().getPlannerContext().getMetadata();
        return new PlanOptimizer.Context(
                session,
                false,
                emptySymbolAllocator(),
                new PlanNodeIdAllocator(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                new CachingTableStatsProvider(metadata, session, () -> false),
                RuntimeInfoProvider.noImplementation(),
                mvSubstitutionCollector);
    }

    private static Type columnType(String tableName, String columnName)
    {
        // coercion_source.name is a bounded varchar while the storage table's name column is unbounded
        // varchar. This lets a single query exercise the optimizer's storage-column type coercion.
        if (tableName.equals("coercion_source") && columnName.equals("name")) {
            return createVarcharType(10);
        }
        return VARCHAR;
    }

    private static ConnectorFactory substitutionConnectorFactory()
    {
        MockConnectorFactory delegate = MockConnectorFactory.builder()
                .withListSchemaNames(_ -> ImmutableList.of(SCHEMA))
                .withListTables((_, _) -> ImmutableList.copyOf(TABLE_COLUMNS.keySet()))
                .withGetColumns(schemaTableName -> TABLE_COLUMNS.getOrDefault(schemaTableName.getTableName(), ImmutableList.of()).stream()
                        .map(column -> new ColumnMetadata(column, columnType(schemaTableName.getTableName(), column)))
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
