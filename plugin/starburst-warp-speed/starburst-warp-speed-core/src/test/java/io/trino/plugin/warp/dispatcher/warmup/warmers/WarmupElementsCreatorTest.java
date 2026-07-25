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
package io.trino.plugin.warp.dispatcher.warmup.warmers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.ExportState;
import io.trino.plugin.warp.dispatcher.model.RecordData;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.type.JsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.createRegularColumns;
import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockColumns;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator.INVALID_WARM_ID;
import static io.trino.plugin.warp.gen.constants.RecTypeCode.REC_TYPE_INTEGER;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_BASIC;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_DATA;
import static io.trino.plugin.warp.gen.constants.WarmUpType.WARM_UP_TYPE_LUCENE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class WarmupElementsCreatorTest
{
    private SchemaTableName schemaTableName;
    private DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private WarmupElementsCreator warmupElementsCreator;

    @BeforeEach
    @SuppressWarnings("MockNotUsedInProduction")
    public void before()
    {
        schemaTableName = new SchemaTableName("schema", "table");
        dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        warmupElementsCreator = new WarmupElementsCreator(
                mock(RowGroupDataService.class),
                mock(MetricsManager.class),
                mock(StorageEngineConstants.class),
                mock(BufferAllocator.class),
                new TestingConnectorProxiedConnectorTransformer(),
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
    }

    @Test
    public void testAllocateByPriorityDifferentColumns()
    {
        String high = "high";
        String medium = "medium";
        String low = "low";

        List<ColumnHandle> columns = mockColumns(List.of(
                Pair.of(high, IntegerType.INTEGER),
                Pair.of(medium, IntegerType.INTEGER),
                Pair.of(low, IntegerType.INTEGER)));

        SetMultimap<WarpColumn, WarmupProperties> warmupProperties = HashMultimap.create();
        warmupProperties.putAll(
                new RegularColumn(high),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 3, 0, TransformFunction.NONE)));
        warmupProperties.putAll(
                new RegularColumn(medium),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE)));
        warmupProperties.putAll(
                new RegularColumn(low),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 1, 0, TransformFunction.NONE)));

        List<WarmUpElement> warmupElements = warmupElementsCreator.createWarmupElements(
                new RowGroupKey("schema", "table", "file_path", 0, 1L, 0, "", ""),
                warmupProperties,
                schemaTableName,
                columns);

        assertThat(warmupElements).containsExactlyInAnyOrder(
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(medium))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(high))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(low))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build());
    }

    /**
     * createWarmupElements method doesn't consider the priority ,unless the priority prevent by demoter.
     * removal elements with low priority is done before this method in filterRequiredCoWarmupTypeMap method.
     * CloudImporter doesn't need to consider the priority order, so creation of required warmup elements is done in recordDataList order.
     * sorting warmupElements by valid priority is done in WarpProxiedWarmer
     */
    @Test
    public void testAllocateByPrioritySamePriorityOrderByColId()
    {
        String high = "high";
        String medium = "medium";
        String low = "low";
        List<ColumnHandle> columns = mockColumns(List.of(
                Pair.of(low, IntegerType.INTEGER),
                Pair.of(medium, IntegerType.INTEGER),
                Pair.of(high, IntegerType.INTEGER)));

        SetMultimap<WarpColumn, WarmupProperties> warmupProperties = HashMultimap.create();
        warmupProperties.putAll(
                new RegularColumn(medium),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE),
                        new WarmupProperties(WARM_UP_TYPE_LUCENE, 2, 0, TransformFunction.NONE),
                        new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE)));

        warmupProperties.putAll(
                new RegularColumn(high),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 3, 0, TransformFunction.NONE)));

        warmupProperties.putAll(
                new RegularColumn(low),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 1, 0, TransformFunction.NONE)));

        List<WarmUpElement> warmupElements = warmupElementsCreator.createWarmupElements(
                new RowGroupKey("schema", "table", "file_path", 0, 1L, 0, "", ""),
                warmupProperties,
                schemaTableName,
                columns);

        assertThat(warmupElements).containsExactlyInAnyOrder(
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(medium))
                        .warmUpType(WARM_UP_TYPE_LUCENE)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(medium))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(medium))
                        .warmUpType(WARM_UP_TYPE_BASIC)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(high))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(low))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .state(WarmUpElementState.VALID)
                        .build());
    }

    @Test
    public void testUnsupportedColumnType()
    {
        String high = "high";
        String medium = "medium";
        String low = "low";
        List<ColumnHandle> columns = mockColumns(List.of(
                Pair.of(low, IntegerType.INTEGER),
                Pair.of(medium, JsonType.JSON), // json type is not supported
                Pair.of(high, IntegerType.INTEGER)));

        SetMultimap<WarpColumn, WarmupProperties> warmupProperties = HashMultimap.create();
        warmupProperties.putAll(
                new RegularColumn(medium),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 2, 0, TransformFunction.NONE),
                        new WarmupProperties(WARM_UP_TYPE_LUCENE, 2, 0, TransformFunction.NONE),
                        new WarmupProperties(WARM_UP_TYPE_BASIC, 2, 0, TransformFunction.NONE)));

        warmupProperties.putAll(
                new RegularColumn(high),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 3, 0, TransformFunction.NONE)));

        warmupProperties.putAll(
                new RegularColumn(low),
                Set.of(new WarmupProperties(WARM_UP_TYPE_DATA, 1, 0, TransformFunction.NONE)));

        List<WarmUpElement> warmupElements = warmupElementsCreator.createWarmupElements(
                new RowGroupKey("schema", "table", "file_path", 0, 1L, 0, "", ""),
                warmupProperties,
                schemaTableName,
                columns);

        assertThat(warmupElements).containsExactlyInAnyOrder(
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(high))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .state(WarmUpElementState.VALID)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .build(),
                WarmUpElement.builder()
                        .recTypeCode(REC_TYPE_INTEGER)
                        .recTypeLength(4)
                        .warpColumn(new RegularColumn(low))
                        .warmUpType(WARM_UP_TYPE_DATA)
                        .warmId(INVALID_WARM_ID)
                        .exportState(ExportState.NOT_EXPORTED)
                        .warmupElementStats(WarmupElementStats.UNINITIALIZED)
                        .state(WarmUpElementState.VALID)
                        .build());
    }

    @Test
    void testCreateRegularColumnRecordData()
    {
        String columnName = "c1";
        Type columnType = IntegerType.INTEGER;
        List<Pair<String, Type>> columnsMetadata = List.of(Pair.of(columnName, columnType));
        List<ColumnHandle> columnHandles = mockColumns(columnsMetadata);
        List<WarpColumn> warpColumns = createRegularColumns(
                columnHandles,
                dispatcherProxiedConnectorTransformer);

        List<RecordData> recordDataList = warmupElementsCreator.getRecordData(
                schemaTableName,
                columnHandles,
                Set.copyOf(warpColumns));

        RecordData expectedRecordData = new RecordData(
                new SchemaTableColumn(schemaTableName, columnName),
                columnType,
                REC_TYPE_INTEGER,
                4);

        assertThat(recordDataList).containsExactly(expectedRecordData);
    }
}
