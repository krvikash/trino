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
package io.trino.plugin.warp.warmup;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.DefaultFakeConnectorSessionProvider;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.warmup.model.PartitionValueWarmupPredicateRule;
import io.trino.plugin.warp.warmup.model.WarmupPredicateRule;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.plugin.warp.warmup.model.WarmupRuleResult;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.JsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WarmupRuleServiceTest
{
    public static final String COL1 = "col1";

    private WarmupRuleService warmupRuleService;
    private Map<String, ColumnHandle> columnMap;
    private ConnectorTableHandle tableHandle;
    private ConnectorMetadata connectorMetadata;
    private StorageEngineConstants storageEngineConstants;
    private GlobalConfig globalConfig;

    @BeforeEach
    public void before()
    {
        columnMap = new HashMap<>();

        storageEngineConstants = spy(new StubsStorageEngineConstants(1000));
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = mock(DispatcherProxiedConnectorTransformer.class);
        connectorMetadata = mock(ConnectorMetadata.class);
        ConnectorTransactionHandle connectorTransactionHandle = mock(ConnectorTransactionHandle.class);
        Connector proxiedConnector = mock(Connector.class);
        when(dispatcherProxiedConnectorTransformer.createProxiedMetadata(eq(proxiedConnector), any(ConnectorSession.class)))
                .thenReturn(Pair.of(connectorMetadata, connectorTransactionHandle));
        tableHandle = mock(ConnectorTableHandle.class);
        when(connectorMetadata.getTableHandle(any(ConnectorSession.class), any(SchemaTableName.class), eq(Optional.empty()), eq(Optional.empty()))).thenAnswer(_ -> tableHandle);
        when(connectorMetadata.getColumnHandles(any(ConnectorSession.class), eq(tableHandle))).thenAnswer((_) -> columnMap);
        globalConfig = new GlobalConfig();
        warmupRuleService = new WarmupRuleService(proxiedConnector,
                storageEngineConstants,
                new WarmupDemoterConfig(),
                dispatcherProxiedConnectorTransformer,
                new DefaultFakeConnectorSessionProvider(Collections.emptyList()),
                globalConfig);
    }

    @Test
    public void testSimpleCRUD()
    {
        createColumn(VarcharType.createVarcharType(10));

        WarmupRule warmupRule = warmupRuleService.save(List.of(createRule(WarmUpType.WARM_UP_TYPE_LUCENE)))
                .appliedRules()
                .getFirst();

        Collection<WarmupRule> allRules = warmupRuleService.getAll();
        assertThat(allRules).containsExactly(warmupRule);

        warmupRuleService.delete(allRules.stream().map(WarmupRule::getId).collect(Collectors.toList()));
        assertThat(warmupRuleService.getAll()).isEmpty();

        assertThat(warmupRuleService.save(List.of(createRule(WarmUpType.WARM_UP_TYPE_LUCENE))).appliedRules())
                .hasSize(1);
        assertThat(warmupRuleService.replaceAll(List.of()).appliedRules()).isEmpty();
    }

    @Test
    public void testRejectIndexRulesWhenDataOnlyFlagIsOn()
    {
        globalConfig.setDataOnlyWarming(true);
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules()).isEmpty();
        assertThat(warmupRuleResult.rejectedRules()).isNotEmpty();
    }

    @Test
    public void testSameUniqueConstraint()
    {
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule1 = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        WarmupRule warmupRule2 = WarmupRule.builder(warmupRule1)/*.id(0)*/.build();

        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule1, warmupRule2));
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                .contains(Integer.toString(WarpErrorCode.WARP_DUPLICATE_RECORD.getCode()))).isTrue();
    }

    @Test
    public void testWarmupRuleId()
    {
        createColumn(VarcharType.createVarcharType(10));
        WarmupRule warmupRule1 = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);

        WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule1));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.appliedRules().getFirst().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_LUCENE);

        //update existing rule
        WarmupRule updatedWarmupRule1 = WarmupRule.builder(warmupRule1)
                .warmUpType(WarmUpType.WARM_UP_TYPE_DATA)
                .build();
        warmupRuleResult = warmupRuleService.save(List.of(updatedWarmupRule1));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.appliedRules().getFirst().getWarmUpType()).isEqualTo(WarmUpType.WARM_UP_TYPE_DATA);

        //now try to save a new rule with non-existing id
        WarmupRule warmupRule3 = WarmupRule.builder(warmupRule1)
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .id(new Random().nextInt(10000, 100000))
                .build();
        warmupRuleResult = warmupRuleService.save(List.of(warmupRule3));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(0);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules()
                .keySet()
                .stream()
                .findAny()
                .orElseThrow()
                .getWarmUpType())
                .isEqualTo(warmupRule3.getWarmUpType());
        assertThat(warmupRuleResult.rejectedRules()
                .entrySet()
                .stream()
                .findFirst()
                .orElseThrow()
                .getValue()
                .stream()
                .findAny()
                .orElseThrow()
                .contains(Integer.toString(WarpErrorCode.WARP_WARMUP_RULE_ID_NOT_VALID.getCode())))
                .isTrue();
    }

    @Test
    public void testWarmupTypeDoesntSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(RowType.rowType(RowType.field(baseType)), "doesn't support column type row",
                new ArrayType(baseType), "doesn't support column type array",
                new MapType(baseType, baseType, new TypeOperators()), "doesn't support column type map",
                JsonType.JSON, "doesn't support column type json");
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_BASIC);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(WarpErrorCode.WARP_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testLuceneWarmupTypeDoesntSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(RowType.rowType(RowType.field(baseType)), "doesn't support column type row",
                new MapType(baseType, baseType, new TypeOperators()), "doesn't support column type map",
                JsonType.JSON, "doesn't support column type json");
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(WarpErrorCode.WARP_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testLuceneWarmupTypeSupportColType()
    {
        Type baseType = VarcharType.createVarcharType(10);

        Set<Type> supportedTypeToMessage = Set.of(new ArrayType(baseType));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE);
        supportedTypeToMessage.forEach((type) -> {
            createColumn(type);
            assertRuleApplied(warmupRule);
        });
    }

    @Test
    public void testWarmupTypeDoesntSupportDataColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        Map<Type, String> unsupportedTypeToMessage = Map.of(new ArrayType(RowType.rowType(RowType.field(baseType))), "doesn't support column type array");

        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA);

        for (Map.Entry<Type, String> typeToMessage : unsupportedTypeToMessage.entrySet()) {
            createColumn(typeToMessage.getKey());
            WarmupRuleResult warmupRuleResult = warmupRuleService.save(List.of(warmupRule));
            assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(Integer.toString(WarpErrorCode.WARP_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode()))).isTrue();
            assertThat(warmupRuleResult.rejectedRules().entrySet().stream().findFirst().orElseThrow().getValue().stream().findAny().orElseThrow()
                    .contains(typeToMessage.getValue())).isTrue();
        }
    }

    @Test
    public void testNeverOnUnsupportedColType()
    {
        Type baseType = VarcharType.createVarcharType(10);
        List<Type> unsupportedTypes = ImmutableList.of(RowType.rowType(RowType.field(baseType)),
                new ArrayType(baseType),
                new MapType(baseType, baseType, new TypeOperators()),
                JsonType.JSON);
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_BASIC);

        for (int i = 0, unsupportedTypesSize = unsupportedTypes.size(); i < unsupportedTypesSize; i++) {
            Type type = unsupportedTypes.get(i);
            String columnName = String.valueOf(i);
            createColumn(columnName, type);
            WarmupRule neverRule = WarmupRule.builder(warmupRule).warpColumn(new RegularColumn(columnName)).priority(-10).build();
            assertRuleApplied(neverRule);
        }
    }

    @Test
    public void testUnknownColumn()
    {
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_LUCENE,
                Set.of(new PartitionValueWarmupPredicateRule("col2", "2")));
        assertRuleRejected(warmupRule, "Rule refer to a non-exist column");
    }

    @Test
    public void testLongCharShouldFail()
    {
        when(storageEngineConstants.getMaxRecLen()).thenReturn(8);
        createColumn(CharType.createCharType(255));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA, Collections.emptySet());
        assertRuleRejected(warmupRule, "Can't warm long Char columns");
    }

    @Test
    public void testCharRule()
    {
        when(storageEngineConstants.getMaxRecLen()).thenReturn(8);
        createColumn(CharType.createCharType(7));
        WarmupRule warmupRule = createRule(WarmUpType.WARM_UP_TYPE_DATA, Collections.emptySet());
        assertRuleApplied(warmupRule);
    }

    @Test
    public void testUniquenessBetweenSaves()
    {
        createColumn(VarcharType.createVarcharType(10));

        WarmupRule warmupRule1 = WarmupRule.builder()
                .schema("bundle")
                .table("bundle")
                .warpColumn(new RegularColumn(COL1))
                .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                .priority(0)
                .ttl(0)
                .predicates(Set.of())
                .build();

        warmupRuleService.save(List.of(warmupRule1));

        try {
            warmupRuleService.save(List.of(warmupRule1));
        }
        catch (TrinoException e) {
            assertThat(e.getErrorCode()).isEqualTo(WarpErrorCode.WARP_DUPLICATE_RECORD.toErrorCode());
        }
    }

    public WarmupRule createRule(WarmUpType warmUpType)
    {
        WarpColumn defaultWarpColumn = new RegularColumn(COL1);
        Set<WarmupPredicateRule> defaultPredicates = Set.of(new PartitionValueWarmupPredicateRule(COL1, "2"));
        return createRule(warmUpType, defaultWarpColumn, defaultPredicates);
    }

    public WarmupRule createRule(WarmUpType warmUpType, Set<WarmupPredicateRule> predicates)
    {
        WarpColumn defaultWarpColumn = new RegularColumn(COL1);
        return createRule(warmUpType, defaultWarpColumn, predicates);
    }

    private WarmupRule createRule(WarmUpType warmUpType, WarpColumn warpColumn, Set<WarmupPredicateRule> predicates)
    {
        return WarmupRule.builder()
                .schema("schema")
                .table("table")
                .warpColumn(warpColumn)
                .warmUpType(warmUpType)
                .priority(0)
                .ttl(0)
                //.id(0)
                .predicates(predicates)
                .build();
    }

    private void createColumn(Type type)
    {
        createColumn(COL1, type);
    }

    private void createColumn(String name, Type type)
    {
        ColumnHandle columnHandle = mock(ColumnHandle.class);
        columnMap.put(name, columnHandle);
        ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
        when(columnMetadata.getName()).thenReturn(name);
        when(columnMetadata.getType()).thenReturn(type);
        when(connectorMetadata.getColumnMetadata(any(ConnectorSession.class), eq(tableHandle), eq(columnHandle))).thenReturn(columnMetadata);
    }

    private void assertRuleApplied(WarmupRule warmupRule)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(ImmutableList.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(1);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(0);
    }

    private void assertRuleRejected(WarmupRule warmupRule, String... expectedRejects)
    {
        WarmupRuleResult warmupRuleResult = warmupRuleService.save(ImmutableList.of(warmupRule));
        assertThat(warmupRuleResult.appliedRules().size()).isEqualTo(0);
        assertThat(warmupRuleResult.rejectedRules().size()).isEqualTo(1);
        Optional<Map.Entry<WarmupRule, Set<String>>> rejectedRule = warmupRuleResult.rejectedRules()
                .entrySet()
                .stream()
                .findFirst();
        assertThat(rejectedRule.isPresent()).isTrue();
        Set<String> actualRejects = rejectedRule.orElseThrow().getValue();
        assertThat(actualRejects.size()).isEqualTo(expectedRejects.length);
        Stream.of(expectedRejects)
                .forEach(expected -> assertThat(actualRejects.stream().anyMatch(actual -> actual.contains(expected))).isTrue());
    }
}
