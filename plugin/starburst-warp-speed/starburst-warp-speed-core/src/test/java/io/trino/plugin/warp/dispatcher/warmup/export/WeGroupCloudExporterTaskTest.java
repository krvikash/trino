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
package io.trino.plugin.warp.dispatcher.warmup.export;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.FastWarmingState;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupExportServiceStats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.createRegularWarmupElements;
import static io.trino.plugin.warp.dispatcher.warmup.export.WarmupExportingService.WARMUP_EXPORTER_STAT_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WeGroupCloudExporterTaskTest
{
    private static final RegularColumn column1 = new RegularColumn("c1");
    private static final RegularColumn column2 = new RegularColumn("c2");
    private static final RegularColumn column3 = new RegularColumn("c3");

    private RowGroupKey rowGroupKey;
    private String cloudImportExportPath;
    private WorkerTaskExecutorService workerTaskExecutorService;
    private RowGroupDataService rowGroupDataService;
    private WarmupElementsCloudExporter warmupElementsCloudExporter;
    private GlobalConfig globalConfig;
    private WarmupExportServiceStats warmupExportServiceStats;

    @BeforeEach
    void setUp()
    {
        rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        workerTaskExecutorService = mock(WorkerTaskExecutorService.class);

        Multimap<WarpColumn, WarmUpType> columnNameToWarmUpType = ArrayListMultimap.create();
        columnNameToWarmUpType.put(column1, WarmUpType.WARM_UP_TYPE_BASIC);
        columnNameToWarmUpType.put(column2, WarmUpType.WARM_UP_TYPE_BASIC);
        columnNameToWarmUpType.put(column3, WarmUpType.WARM_UP_TYPE_BASIC);

        List<WarmUpElement> warmupElements = createRegularWarmupElements(columnNameToWarmUpType);
        RowGroupData rowGroupData = createRowGroup(warmupElements);

        rowGroupDataService = mock(RowGroupDataService.class);
        when(rowGroupDataService.get(rowGroupKey)).thenReturn(rowGroupData);

        warmupElementsCloudExporter = mock(WarmupElementsCloudExporter.class);

        globalConfig = new GlobalConfig();
        globalConfig.setEnableImportExport(true);

        CloudVendorConfig cloudVendorConfig = new CloudVendorConfig();
        cloudVendorConfig.setStoreType("s3");
        cloudVendorConfig.setStorePath("s3://bucket/");

        warmupExportServiceStats = new WarmupExportServiceStats(WARMUP_EXPORTER_STAT_GROUP);

        CloudVendorService cloudVendorService = mock(CloudVendorService.class);
        when(cloudVendorService.getLocation(anyString())).thenCallRealMethod();

        cloudImportExportPath = WarpSessionProperties.getS3ImportExportPath(null, cloudVendorConfig, cloudVendorService);
    }

    @Test
    void test_exportWeGroup()
    {
        when(warmupElementsCloudExporter.exportFile(any(RowGroupData.class), eq(cloudImportExportPath)))
                .thenReturn(new WarmupElementsCloudExporter.ExportFileResults(true, new RowGroupDataValidation(0, 0)));

        ArgumentCaptor<RowGroupData> rowGroupDataCaptor = ArgumentCaptor.forClass(RowGroupData.class);

        WeGroupCloudExporterTask task = new WeGroupCloudExporterTask(rowGroupKey,
                cloudImportExportPath,
                workerTaskExecutorService,
                rowGroupDataService,
                warmupElementsCloudExporter,
                globalConfig,
                warmupExportServiceStats);
        task.run();

        verify(warmupElementsCloudExporter, times(1)).exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        assertThat(warmupExportServiceStats.getexport_row_group_accomplished()).isEqualTo(1);
        assertThat(warmupExportServiceStats.getexport_row_group_finished()).isEqualTo(1);
        assertThat(warmupExportServiceStats.getexport_skipped_due_key_demoted_row_group()).isZero();

        verify(workerTaskExecutorService, times(1)).taskFinished(eq(rowGroupKey));
        verify(rowGroupDataService, times(1)).save(rowGroupDataCaptor.capture());

        Assertions.assertEquals(FastWarmingState.EXPORTED, rowGroupDataCaptor.getValue().getFastWarmingState());
    }

    @Test
    void test_exportWeGroup_fail()
    {
        doThrow(new RuntimeException("test"))
                .when(warmupElementsCloudExporter)
                .exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        WeGroupCloudExporterTask task = new WeGroupCloudExporterTask(rowGroupKey,
                cloudImportExportPath,
                workerTaskExecutorService,
                rowGroupDataService,
                warmupElementsCloudExporter,
                globalConfig,
                warmupExportServiceStats);
        task.run();

        verify(warmupElementsCloudExporter, times(1))
                .exportFile(any(RowGroupData.class), eq(cloudImportExportPath));

        assertThat(warmupExportServiceStats.getexport_row_group_accomplished()).isEqualTo(0);
        assertThat(warmupExportServiceStats.getexport_row_group_failed()).isEqualTo(1);
        assertThat(warmupExportServiceStats.getexport_row_group_finished()).isEqualTo(1);
        assertThat(warmupExportServiceStats.getexport_skipped_due_key_demoted_row_group()).isZero();

        verify(workerTaskExecutorService, times(1)).taskFinished(eq(rowGroupKey));
    }

    private RowGroupData createRowGroup(List<WarmUpElement> warmupElements)
    {
        return RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmupElements)
                .nextOffset(10)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .build();
    }
}
