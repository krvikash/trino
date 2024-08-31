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
package io.trino.plugin.warp.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.connector.TestingConnectorColumnHandle;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.connector.TestingConnectorTableHandle;
import io.trino.plugin.warp.dispatcher.DispatcherMetadata;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandleBuilderProvider;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.ExpressionService;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.ExperimentSupportedFunction;
import io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.NativeExpressionRulesHandler;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.testing.InterfaceTestUtils;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.plugin.warp.WarpSessionProperties.ENABLE_OR_PUSHDOWN;
import static io.trino.plugin.warp.WarpSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumnHandle;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DispatcherMetadataTest
{
    private static final String schemaName = "tmp";
    private static final String tableName = "table";

    private ConnectorSession session;
    DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private ExpressionService expressionService;
    private DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;

    @BeforeEach
    public void before()
    {
        MetricsManager metricsManager = TestingTxService.createMetricsManager();
        session = mock(ConnectorSession.class);
        when(session.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(5);
        GlobalConfig globalConfig = new GlobalConfig();
        dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        dispatcherTableHandleBuilderProvider = new DispatcherTableHandleBuilderProvider(dispatcherProxiedConnectorTransformer);
        NativeExpressionRulesHandler nativeExpressionRulesHandler = new NativeExpressionRulesHandler(new StubsStorageEngineConstants(), metricsManager);
        expressionService = new ExpressionService(dispatcherProxiedConnectorTransformer,
                new ExperimentSupportedFunction(metricsManager),
                globalConfig,
                new NativeConfig(),
                metricsManager,
                nativeExpressionRulesHandler);
    }

    @Test
    public void testEverythingImplemented()
            throws NoSuchMethodException
    {
        InterfaceTestUtils.assertAllMethodsOverridden(
                ConnectorMetadata.class,
                DispatcherMetadata.class,
                Set.of(
                        // Deprecated methods
                        ConnectorMetadata.class.getMethod("applyJoin", ConnectorSession.class, JoinType.class, ConnectorTableHandle.class, ConnectorTableHandle.class, List.class, Map.class, Map.class, JoinStatistics.class),
                        ConnectorMetadata.class.getMethod("streamTableColumns", ConnectorSession.class, SchemaTablePrefix.class),
                        ConnectorMetadata.class.getMethod("listTableColumns", ConnectorSession.class, SchemaTablePrefix.class)));
    }

    @Test
    @Disabled(value = "testApplyFilterWithoutLucene->need to check why it fails")
    public void testApplyFilterWithoutLucene()
    {
        ColumnHandle columnHandleCol1 = mockColumnHandle("col1bigint", BIGINT, dispatcherProxiedConnectorTransformer);
        TupleDomain<ColumnHandle> predicate1 = TupleDomain.withColumnDomains(Map.of(columnHandleCol1, Domain.singleValue(BIGINT, 1L)));
        Constraint constraint1 = new Constraint(predicate1);

        ColumnHandle columnHandleCol2 = mockColumnHandle("col2boolean", BOOLEAN, dispatcherProxiedConnectorTransformer);
        TupleDomain<ColumnHandle> predicate2 = TupleDomain.withColumnDomains(Map.of(columnHandleCol2, Domain.singleValue(BOOLEAN, true)));
        Constraint constraint2 = new Constraint(predicate2);

        ConnectorMetadata hiveMetadata = mockHiveMetadata();
        mockHiveApplyFilter(hiveMetadata, predicate1);
        DispatcherMetadata dispatcherMetadata = new DispatcherMetadata(
                hiveMetadata,
                expressionService,
                dispatcherTableHandleBuilderProvider,
                new GlobalConfig());

        // Apply predicate pushdown on the first column.
        DispatcherTableHandle dispatcherTableHandle = createDispatcherTableHandle();
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result1 = dispatcherMetadata.applyFilter(
                session, dispatcherTableHandle, constraint1);

        assertThat(result1.isPresent()).isTrue();
        assertThat(((TestingConnectorTableHandle) result1.orElseThrow().getAlternatives().getFirst().handle()).getSchemaName()).isEqualTo(schemaName);
        assertThat(((TestingConnectorTableHandle) result1.orElseThrow().getAlternatives().getFirst().handle()).getTableName()).isEqualTo(tableName);
        assertThat(((TestingConnectorTableHandle) result1.orElseThrow().getAlternatives().getFirst().handle()).getCompactEffectivePredicate()).isEqualTo(predicate1);

        // Predicate is not pushed down when Hive doesn't push down
        when(hiveMetadata.applyFilter(any(), any(), any())).thenReturn(Optional.empty());
        assertThat(dispatcherMetadata.applyFilter(session, result1.orElseThrow().getAlternatives().getFirst().handle(), constraint1)).isEmpty();

        // Pushdown another predicate into the resulting table from above (to test the intersection with existing predicate).
        mockHiveApplyFilter(hiveMetadata, predicate1.intersect(predicate2));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result2 = dispatcherMetadata.applyFilter(
                session, result1.orElseThrow().getAlternatives().getFirst().handle(), constraint2);

        assertThat(result2).isPresent();
        assertThat(((TestingConnectorTableHandle) result2.orElseThrow().getAlternatives().getFirst().handle()).getCompactEffectivePredicate()).isEqualTo(predicate1.intersect(predicate2));

        // Pushing down a more generic predicate should not restrict the scan result.
        when(hiveMetadata.applyFilter(any(), any(), any())).thenReturn(Optional.empty());
        TupleDomain<ColumnHandle> predicate3 = TupleDomain.withColumnDomains(Map.of(
                columnHandleCol1, Domain.multipleValues(BIGINT, List.of(1L, 2L, 3L))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result3 = dispatcherMetadata.applyFilter(
                session, result2.orElseThrow().getAlternatives().getFirst().handle(), new Constraint(predicate3));

        assertThat(result3).isEmpty();

        // Pushing down conflicting predicates should result in an empty scan result.
        mockHiveApplyFilter(hiveMetadata, TupleDomain.none());
        TupleDomain<ColumnHandle> predicate4 = TupleDomain.withColumnDomains(Map.of(
                columnHandleCol2, Domain.singleValue(BOOLEAN, false)));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result4 = dispatcherMetadata.applyFilter(
                session, result2.orElseThrow().getAlternatives().getFirst().handle(), new Constraint(predicate4));

        assertThat(result4).isPresent();
        assertThat(((TestingConnectorTableHandle) result4.orElseThrow().getAlternatives().getFirst().handle()).getEnforcedConstraint()).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testApplyFilterWarpExpressionAndDomain()
    {
        TestingConnectorColumnHandle columnHandleCol1 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col1like");
        TestingConnectorColumnHandle columnHandleCol2 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col2like");
        String columnName1 = columnHandleCol1.name();
        Map<String, ColumnHandle> assignments = Map.of(columnName1, columnHandleCol1,
                columnHandleCol2.name(), columnHandleCol2);
        Slice pattern = Slices.utf8Slice("%hello%");

        // Test a WarpExpression
        TupleDomain<ColumnHandle> predicate = TupleDomain.all();
        ConnectorExpression connectorExpression = createLikeCall(columnHandleCol1, pattern);
        WarpExpression expectedWarpExpression = createLikeWarpCall(columnHandleCol1, pattern);
        RegularColumn warpColumn1 = new RegularColumn(columnName1);

        List<WarpExpressionData> expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpression, VarcharType.VARCHAR, false, Optional.empty(), warpColumn1));
        runApplyFilterWarpExpressionTestCase(predicate, connectorExpression, assignments, predicate, expectedWarpExpressions);

        // Test a single value
        Domain domain = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("aa"));
        predicate = TupleDomain.withColumnDomains(Map.of(columnHandleCol2, domain));
        connectorExpression = Constant.TRUE;
        expectedWarpExpressions = Collections.emptyList();
        runApplyFilterWarpExpressionTestCase(predicate, connectorExpression, assignments, predicate, expectedWarpExpressions);

        // Test range
        Range range = Range.range(columnHandleCol2.type(), Slices.utf8Slice("aa"), false, Slices.utf8Slice("ad"), false);
        domain = Domain.create(ValueSet.ofRanges(range), false);
        predicate = TupleDomain.withColumnDomains(Map.of(columnHandleCol2, domain));

        runApplyFilterWarpExpressionTestCase(predicate, connectorExpression, assignments, predicate, expectedWarpExpressions);

        // Test multiple columns
        domain = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("aa"));
        predicate = TupleDomain.withColumnDomains(Map.of(columnHandleCol2, domain));
        connectorExpression = createLikeCall(columnHandleCol1, pattern);
        expectedWarpExpression = createLikeWarpCall(columnHandleCol1, pattern);
        expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpression, VarcharType.VARCHAR, false, Optional.empty(), warpColumn1));
        runApplyFilterWarpExpressionTestCase(predicate, connectorExpression, assignments, predicate, expectedWarpExpressions);
    }

    @Test
    public void testApplyFilterWarpExpressionsAndSingleValue()
    {
        TestingConnectorColumnHandle columnHandleCol1 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col1like");
        TestingConnectorColumnHandle columnHandleCol2 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col2like");
        TestingConnectorColumnHandle columnHandleCol3 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col3Single");
        Map<String, ColumnHandle> assignments = Map.of(columnHandleCol1.name(), columnHandleCol1,
                columnHandleCol2.name(), columnHandleCol2);
        Slice pattern = Slices.utf8Slice("%hello%");

        ConnectorExpression connectorExpressionCol1 = createLikeCall(columnHandleCol1, pattern);
        ConnectorExpression connectorExpressionCol2 = createLikeCall(columnHandleCol2, pattern);
        ConnectorExpression connectorExpression = ConnectorExpressions.and(connectorExpressionCol1, connectorExpressionCol2);
        WarpExpression expectedWarpExpressionCol1 = createLikeWarpCall(columnHandleCol1, pattern);
        WarpExpression expectedWarpExpressionCol2 = createLikeWarpCall(columnHandleCol2, pattern);
        RegularColumn regularColumn1 = new RegularColumn(columnHandleCol1.name());
        RegularColumn regularColumn2 = new RegularColumn(columnHandleCol2.name());
        List<WarpExpressionData> expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpressionCol1, VarcharType.VARCHAR, false, Optional.empty(), regularColumn1),
                new WarpExpressionData(expectedWarpExpressionCol2, VarcharType.VARCHAR, false, Optional.empty(), regularColumn2));
        Domain domain = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("aa"));
        TupleDomain<ColumnHandle> predicateCol3 = TupleDomain.withColumnDomains(Map.of(columnHandleCol3, domain));

        runApplyFilterWarpExpressionTestCase(predicateCol3,
                connectorExpression,
                assignments,
                predicateCol3,
                expectedWarpExpressions);
    }

    @Test
    public void testApplyFilterWarpExpressionsAndRange()
    {
        TestingConnectorColumnHandle columnHandleCol1 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col1like");
        TestingConnectorColumnHandle columnHandleCol2 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col2like");
        TestingConnectorColumnHandle columnHandleCol3 = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "col3range");
        Map<String, ColumnHandle> assignments = Map.of(columnHandleCol1.name(), columnHandleCol1,
                columnHandleCol2.name(), columnHandleCol2);
        Slice pattern = Slices.utf8Slice("%hello%");

        ConnectorExpression connectorExpressionCol1 = createLikeCall(columnHandleCol1, pattern);
        ConnectorExpression connectorExpressionCol2 = createLikeCall(columnHandleCol2, pattern);
        ConnectorExpression connectorExpression = ConnectorExpressions.and(connectorExpressionCol1, connectorExpressionCol2);
        WarpExpression expectedWarpExpressionCol1 = createLikeWarpCall(columnHandleCol1, pattern);
        WarpExpression expectedWarpExpressionCol2 = createLikeWarpCall(columnHandleCol2, pattern);
        List<WarpExpressionData> expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpressionCol1, VarcharType.VARCHAR, false, Optional.empty(), new RegularColumn(columnHandleCol1.name())),
                new WarpExpressionData(expectedWarpExpressionCol2, VarcharType.VARCHAR, false, Optional.empty(), new RegularColumn(columnHandleCol2.name())));
        Range range = Range.range(columnHandleCol2.type(), Slices.utf8Slice("aa"), false, Slices.utf8Slice("ad"), false);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);
        TupleDomain<ColumnHandle> predicateCol3 = TupleDomain.withColumnDomains(Map.of(columnHandleCol3, domain));

        runApplyFilterWarpExpressionTestCase(predicateCol3,
                connectorExpression,
                assignments,
                predicateCol3,
                expectedWarpExpressions);
    }

    @Disabled("testApplyFilterTwoIterations->VDB-5850")
    @Test
    public void testApplyFilterTwoIterations()
    {
        TestingConnectorColumnHandle columnHandleCol1 = mockColumnHandle("lucene1col", VarcharType.VARCHAR, dispatcherProxiedConnectorTransformer);
        TestingConnectorColumnHandle columnHandleCol2 = mockColumnHandle("lucene2col", VarcharType.VARCHAR, dispatcherProxiedConnectorTransformer);
        String column1Name = columnHandleCol1.name();
        Map<String, ColumnHandle> assignments = Map.of(column1Name, columnHandleCol1,
                columnHandleCol2.name(), columnHandleCol2);
        RegularColumn regularColumn1 = new RegularColumn(column1Name);
        Slice pattern1 = Slices.utf8Slice("%hello%");
        Slice pattern2 = Slices.utf8Slice("%hello2%");
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> constraintApplicationResult;

        // Test first domain + expression
        ConnectorExpression connectorExpression1 = createLikeCall(columnHandleCol1, pattern1);
        ValueSet sortedRangeSet1 = ValueSet.ofRanges(Range.greaterThan(VarcharType.VARCHAR, Slices.utf8Slice("a")));
        Map<ColumnHandle, Domain> columnDomains1 = Map.of(columnHandleCol1, Domain.create(sortedRangeSet1, true));
        TupleDomain<ColumnHandle> predicate1 = TupleDomain.withColumnDomains(columnDomains1);
        WarpExpression expectedWarpExpressionCol1 = createLikeWarpCall(columnHandleCol1, pattern1);
        TupleDomain<ColumnHandle> expectedPredicate = predicate1;
        List<WarpExpressionData> expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpressionCol1, VarcharType.VARCHAR, false, Optional.empty(), regularColumn1));
        constraintApplicationResult = runApplyFilterWarpExpressionTestCase(predicate1,
                connectorExpression1,
                assignments,
                expectedPredicate,
                expectedWarpExpressions);

        // Test another domain and another expression on an already exists column
        ConnectorExpression connectorExpression2 = createLikeCall(columnHandleCol1, pattern2);
        ValueSet sortedRangeSet2 = ValueSet.ofRanges(Range.greaterThan(VarcharType.VARCHAR, Slices.utf8Slice("b")));
        Map<ColumnHandle, Domain> columnDomains2 = Map.of(columnHandleCol1, Domain.create(sortedRangeSet2, false));
        TupleDomain<ColumnHandle> predicate2 = TupleDomain.withColumnDomains(columnDomains2);
        expectedPredicate = expectedPredicate.intersect(predicate2);
        expectedWarpExpressionCol1 = andWarpExpressions(expectedWarpExpressionCol1,
                createLikeWarpCall(columnHandleCol1, pattern2));
        expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpressionCol1, VarcharType.VARCHAR, false, Optional.empty(), regularColumn1));
        DispatcherTableHandle tableHandle = (DispatcherTableHandle) constraintApplicationResult.orElseThrow().getAlternatives().getFirst().handle();
        constraintApplicationResult = runApplyFilterWarpExpressionTestCase(predicate2,
                connectorExpression2,
                assignments,
                expectedPredicate,
                expectedWarpExpressions,
                tableHandle);

        // Test another domain and expression on an a different column
        ConnectorExpression connectorExpression3 = createLikeCall(columnHandleCol2, pattern1);
        ValueSet sortedRangeSet3 = ValueSet.ofRanges(Range.greaterThan(VarcharType.VARCHAR, Slices.utf8Slice("c")));
        Map<ColumnHandle, Domain> columnDomains3 = Map.of(columnHandleCol2, Domain.create(sortedRangeSet3, false));
        TupleDomain<ColumnHandle> predicate3 = TupleDomain.withColumnDomains(columnDomains3);
        expectedPredicate = expectedPredicate.intersect(predicate3);
        WarpExpression expectedWarpExpressionCol2 = createLikeWarpCall(columnHandleCol2, pattern1);
        RegularColumn regularColumn2 = new RegularColumn(columnHandleCol2.name());
        expectedWarpExpressions = List.of(new WarpExpressionData(expectedWarpExpressionCol1, VarcharType.VARCHAR, false, Optional.empty(), regularColumn1),
                new WarpExpressionData(expectedWarpExpressionCol2, VarcharType.VARCHAR, false, Optional.empty(), regularColumn2));
        tableHandle = (DispatcherTableHandle) constraintApplicationResult.orElseThrow().getAlternatives().getFirst().handle();
        runApplyFilterWarpExpressionTestCase(predicate3, connectorExpression3, assignments, expectedPredicate, expectedWarpExpressions, tableHandle);
    }

    private WarpCall andWarpExpressions(WarpExpression expression1, WarpExpression expression2)
    {
        ImmutableList<WarpExpression> warpExpressions = ImmutableList.of(expression1, expression2);
        return new WarpCall(StandardFunctions.AND_FUNCTION_NAME.getName(), warpExpressions, BOOLEAN);
    }

    private Call createLikeCall(TestingConnectorColumnHandle columnHandleCol, Slice pattern)
    {
        return new Call(BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(new Variable(columnHandleCol.name(), columnHandleCol.type()),
                        new Constant(pattern, VarcharType.VARCHAR)));
    }

    private WarpCall createLikeWarpCall(TestingConnectorColumnHandle columnHandleCol, Slice pattern)
    {
        return new WarpCall(LIKE_FUNCTION_NAME.getName(),
                ImmutableList.of(new WarpVariable(columnHandleCol, columnHandleCol.type()),
                        new WarpSliceConstant(pattern, VarcharType.VARCHAR)),
                BOOLEAN);
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> runApplyFilterWarpExpressionTestCase(
            TupleDomain<ColumnHandle> predicate,
            ConnectorExpression connectorExpression,
            Map<String, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> expectedPredicate,
            List<WarpExpressionData> expectedWarpExpressions)
    {
        DispatcherTableHandle dispatcherTableHandle = createDispatcherTableHandle();
        return runApplyFilterWarpExpressionTestCase(predicate, connectorExpression, assignments, expectedPredicate, expectedWarpExpressions, dispatcherTableHandle);
    }

    private Optional<ConstraintApplicationResult<ConnectorTableHandle>> runApplyFilterWarpExpressionTestCase(
            TupleDomain<ColumnHandle> predicate,
            ConnectorExpression connectorExpression,
            Map<String, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> expectedPredicate,
            List<WarpExpressionData> expectedWarpExpressions,
            DispatcherTableHandle dispatcherTableHandle)
    {
        Constraint constraint = new Constraint(predicate, connectorExpression, assignments);

        ConnectorMetadata proxyMetadata = mockHiveMetadata();
        mockHiveApplyFilter(proxyMetadata, predicate);
        DispatcherMetadata dispatcherMetadata = new DispatcherMetadata(
                proxyMetadata,
                expressionService,
                dispatcherTableHandleBuilderProvider,
                new GlobalConfig());
        when(session.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = dispatcherMetadata.applyFilter(
                session, dispatcherTableHandle, constraint);

        assertThat(result.isPresent()).isTrue();
        assertThat(((DispatcherTableHandle) result.orElseThrow().getAlternatives().getFirst().handle()).getSchemaName()).isEqualTo(schemaName);
        assertThat(((DispatcherTableHandle) result.orElseThrow().getAlternatives().getFirst().handle()).getTableName()).isEqualTo(tableName);
        assertThat(((DispatcherTableHandle) result.orElseThrow().getAlternatives().getFirst().handle()).getFullPredicate()).isEqualTo(expectedPredicate);
        if (expectedWarpExpressions.isEmpty()) {
            assertThat(((DispatcherTableHandle) result.orElseThrow().getAlternatives().getFirst().handle()).getWarpExpression()).isEmpty();
        }
        else {
            assertThat(((DispatcherTableHandle) result.orElseThrow().getAlternatives().getFirst().handle()).getWarpExpression().orElseThrow().warpExpressionDataLeaves()).containsExactlyInAnyOrderElementsOf(expectedWarpExpressions);
        }
        return result;
    }

    @Test
    public void testApplyLimit()
    {
        DispatcherTableHandle dispatcherTableHandle = createDispatcherTableHandle();
        ConnectorMetadata hiveMetadata = mockHiveMetadata();
        DispatcherMetadata dispatcherMetadata = new DispatcherMetadata(
                hiveMetadata,
                expressionService,
                dispatcherTableHandleBuilderProvider,
                new GlobalConfig());
        Optional<LimitApplicationResult<ConnectorTableHandle>> result = dispatcherMetadata.applyLimit(session, dispatcherTableHandle, 1);
        assertThat(result.isPresent()).isTrue();
        DispatcherTableHandle dispatcherTableHandle1 = (DispatcherTableHandle) result.orElseThrow().getHandle();
        assertThat(dispatcherTableHandle1.getLimit()).isEqualTo(OptionalLong.of(1));
    }

    private DispatcherTableHandle createDispatcherTableHandle()
    {
        return new DispatcherTableHandle(schemaName,
                tableName,
                OptionalLong.empty(),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of()),
                new TestingConnectorTableHandle(
                        schemaName,
                        tableName,
                        new ArrayList<>(),
                        new ArrayList<>(),
                        TupleDomain.all(),
                        TupleDomain.all(),
                        Optional.empty()),
                Optional.empty(),
                Collections.emptyList(),
                false);
    }

    private ConnectorMetadata mockHiveMetadata()
    {
        ConnectorMetadata hiveMetadata = mock(HiveMetadata.class);
        ConnectorTableMetadata connectorTableMetadata = mock(ConnectorTableMetadata.class);
        when(connectorTableMetadata.getProperties()).thenReturn(Map.of(HiveTableProperties.EXTERNAL_LOCATION_PROPERTY, "test",
                HiveTableProperties.STORAGE_FORMAT_PROPERTY, HiveStorageFormat.ORC));
        when(hiveMetadata.getTableMetadata(any(), any())).thenReturn(connectorTableMetadata);
        when(hiveMetadata.applyLimit(any(), any(), anyLong())).thenReturn(Optional.empty());
        return hiveMetadata;
    }

    private void mockHiveApplyFilter(ConnectorMetadata hiveMetadata, TupleDomain<ColumnHandle> predicate)
    {
        TestingConnectorTableHandle tableHandle = new TestingConnectorTableHandle(
                schemaName,
                tableName,
                List.of(),
                List.of(),
                predicate.transformKeys(TestingConnectorColumnHandle.class::cast),
                predicate,
                Optional.empty());

        when(hiveMetadata.applyFilter(any(), any(), any()))
                .thenReturn(Optional.of(new ConstraintApplicationResult<>(
                        false,
                        List.of(new ConstraintApplicationResult.Alternative<>(tableHandle, predicate, Optional.empty(), false)))));
    }
}
