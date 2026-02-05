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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.tools.util.CompressionUtil;
import io.trino.spi.catalog.CatalogName;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WeGroupWarmerTest
{
    private static final Logger log = Logger.get(WeGroupWarmerTest.class);
    private static Path localStorePath;

    private GlobalConfig globalConfig;
    private RowGroupDataService rowGroupDataService;
    private CloudVendorService cloudVendorService;
    private ObjectMapper objectMapper;
    private WarmupImportServiceStats warmupImportServiceStats;
    private final Random random = new Random();
    private WeGroupWarmer weGroupWarmer;

    @BeforeAll
    static void beforeAll()
            throws IOException
    {
        localStorePath = Files.createTempDirectory("WeGroupWarmerTest");
    }

    @AfterAll
    static void afterAll()
    {
        try {
            deleteRecursively(localStorePath, ALLOW_INSECURE);
        }
        catch (IOException e) {
            // TODO this probably should be propagated
            log.error(e, "Failed to delete localStorePath '%s'", localStorePath);
        }
        localStorePath = null;
    }

    @BeforeEach
    void setUp()
    {
        globalConfig = new GlobalConfig();
        globalConfig.setLocalStorePath(localStorePath.toFile().getAbsolutePath());

        CloudVendorConfig cloudVendorConfig = new CloudVendorConfig();
        cloudVendorConfig.setStoreType("s3");
        cloudVendorConfig.setStorePath("s3://store-bucket/");

        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);

        rowGroupDataService = mock(RowGroupDataService.class);

        cloudVendorService = mock(CloudVendorService.class);
        when(cloudVendorService.getLocation(anyString())).thenCallRealMethod();

        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapper = objectMapperProvider.get();

        MetricsManager metricsManager = mock(MetricsManager.class);
        warmupImportServiceStats = WarmupImportServiceStats.create();
        when(metricsManager.registerMetric(any())).thenReturn(warmupImportServiceStats);

        weGroupWarmer = new WeGroupWarmer(globalConfig,
                cloudVendorConfig,
                storageEngineConstants,
                rowGroupDataService,
                cloudVendorService,
                mock(NativeStorageStateHandler.class),
                objectMapperProvider,
                metricsManager,
                new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
    }

    @Test
    void test_isNeedDownload_true()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(RowGroupDataValidation.EMPTY_VALIDATION);
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertTrue(isNeedDownloadResults.isNeedDownload());
        Assertions.assertEquals(storageObjectMetadata.getLastModified().orElseThrow(), isNeedDownloadResults.dataValidation().fileModifiedTime());
        Assertions.assertEquals(storageObjectMetadata.getContentLength().orElseThrow(), isNeedDownloadResults.dataValidation().fileContentLength());
    }

    @Test
    void test_isNeedDownload_AlreadyDownloaded()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(new RowGroupDataValidation(storageObjectMetadata));
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertFalse(isNeedDownloadResults.isNeedDownload());
    }

    @Test
    void test_isNeedDownload_NoFile()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String path = "test-bucket/test-object-name";

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(eq(path))).thenReturn(storageObjectMetadata);

        WeGroupWarmer.IsNeedDownloadResults isNeedDownloadResults = weGroupWarmer.isNeedDownload(rowGroupKey, path);
        Assertions.assertFalse(isNeedDownloadResults.isNeedDownload());
    }

    @Test
    void test_importWeGroup_downloadNotNeeded()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        Optional<RowGroupData> rowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(rowGroupData.isEmpty());
    }

    @Test
    void test_importWeGroup_Exception1()
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(123456789);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        doThrow(new IllegalArgumentException("test-exception")).when(cloudVendorService).downloadFileFromCloud(anyString(), any(File.class));

        RowGroupData rowGroupData = mock(RowGroupData.class);
        when(rowGroupData.getDataValidation()).thenReturn(RowGroupDataValidation.EMPTY_VALIDATION);
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_started());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_failed());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_accomplished());
    }

    @Test
    void test_importWeGroup_Exception2()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-0", "table-0", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        File localFile = new File(localFileName);
        File localTmpFile = new File(localFileName + ".tmp");

        FileUtils.createParentDirectories(localFile);
        boolean unused = localFile.createNewFile();
        unused = localTmpFile.createNewFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(0);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);
        when(cloudVendorService.downloadFileFromCloud(anyString(), any(File.class))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        doAnswer(invocation -> {
            boolean _ = localFile.delete();
            return null;
        }).when(rowGroupDataService).deleteData(eq(rowGroupData), eq(true));

        doThrow(new RuntimeException("test-exception")).when(rowGroupDataService).reload(eq(rowGroupKey), eq(rowGroupData));

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isPresent());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_started());
        Assertions.assertEquals(0, warmupImportServiceStats.getimport_we_group_download_failed());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_accomplished());

        String localSaveFileName = localFileName + ".save";
        File localSaveFile = new File(localSaveFileName);
        Assertions.assertFalse(localSaveFile.exists());

        Assertions.assertTrue(localFile.exists());
        unused = localFile.delete();
        unused = localTmpFile.delete();
    }

    @Test
    void test_importWeGroup_downloadOk()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 0, 0L, 0, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        File localFile = new File(localFileName);
        File localTmpFile = new File(localFileName + ".tmp");

        FileUtils.createParentDirectories(localFile);
        boolean unused = localFile.createNewFile();
        unused = localTmpFile.createNewFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(0);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);
        when(cloudVendorService.downloadFileFromCloud(anyString(), any(File.class))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        doAnswer(invocation -> {
            boolean _ = localFile.delete();
            return null;
        }).when(rowGroupDataService).deleteData(eq(rowGroupData), eq(true));
        when(rowGroupDataService.reload(eq(rowGroupKey), eq(rowGroupData))).thenReturn(rowGroupData);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isPresent());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_started());
        Assertions.assertEquals(0, warmupImportServiceStats.getimport_we_group_download_failed());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_accomplished());

        String localSaveFileName = localFileName + ".save";
        File localSaveFile = new File(localSaveFileName);
        Assertions.assertFalse(localSaveFile.exists());

        Assertions.assertTrue(localFile.exists());
        unused = localFile.delete();
        unused = localTmpFile.delete();
    }

    @Test
    void test_importWeGroup_corruptedFile()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema", "table", "s3://test-bucket/column_split_file", 1, 1L, 1, "", "");
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        File localFile = new File(localFileName);
        File localTmpFile = new File(localFileName + ".tmp");

        FileUtils.createParentDirectories(localFile);
        boolean unused = localFile.createNewFile();
        unused = localTmpFile.createNewFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(Instant.now().toEpochMilli());
        storageObjectMetadata.setContentLength(0);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);
        when(cloudVendorService.downloadFileFromCloud(anyString(), any(File.class))).thenReturn(storageObjectMetadata);

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(List.of())
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);
        doAnswer(invocation -> {
            boolean _ = localFile.delete();
            return null;
        }).when(rowGroupDataService).deleteData(eq(rowGroupData), eq(true));
        when(rowGroupDataService.reload(eq(rowGroupKey), eq(rowGroupData))).thenAnswer(invocation -> {
            boolean _ = localFile.delete();
            return null;
        });

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWeGroup(null, rowGroupKey);

        Assertions.assertTrue(optionalRowGroupData.isPresent());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_started());
        Assertions.assertEquals(0, warmupImportServiceStats.getimport_we_group_download_failed());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_we_group_download_accomplished());

        String localSaveFileName = localFileName + ".save";
        File localSaveFile = new File(localSaveFileName);
        Assertions.assertFalse(localSaveFile.exists());

        Assertions.assertTrue(localFile.exists());
        unused = localFile.delete();
        unused = localTmpFile.delete();
    }

    record TestInitResults(File localFile,
                           int fillerLength,
                           List<WarmUpElement> warmWarmUpElements,
                           RowGroupData rowGroupData)
    {
    }

    TestInitResults testInit(RowGroupKey rowGroupKey)
            throws IOException
    {
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        File localFile = new File(localFileName);

        FileUtils.createParentDirectories(localFile);
        boolean _ = localFile.createNewFile();

        List<WarmUpElement> warmUpElements = new ArrayList<>();
        List<WarmUpElement> warmWarmUpElements = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            WarmUpElement warmUpElement = WarmUpElement.builder()
                    .colName("colName-" + i)
                    .warmUpType(WarmUpType.WARM_UP_TYPE_BASIC)
                    .warmupElementStats(new WarmupElementStats(0, Long.MAX_VALUE, Long.MIN_VALUE))
                    .warmState(((i % 2) == 1) ? WarmState.WARM : WarmState.HOT)
                    .build();
            warmUpElements.add(warmUpElement);
            if ((i % 2) == 1) {
                warmWarmUpElements.add(warmUpElement);
            }
        }

        long lastModified = Instant.now().toEpochMilli();
        long contentLength = 1000;

        RowGroupData rowGroupData = RowGroupData.builder()
                .rowGroupKey(rowGroupKey)
                .warmUpElements(warmUpElements)
                .dataValidation(new RowGroupDataValidation("etag", lastModified, contentLength))
                .build();

        int fillerLength;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(localFile, "rw")) {
            String str = objectMapper.writeValueAsString(rowGroupData);
            byte[] bytes = CompressionUtil.compressGzip(str);
            contentLength = (long) bytes.length + Long.BYTES;
            fillerLength = 1000 - Long.valueOf(contentLength).intValue();
            byte[] filler = new byte[fillerLength];

            random.nextBytes(filler);
            randomAccessFile.write(filler);
            randomAccessFile.write(bytes);
            randomAccessFile.writeLong(Integer.valueOf(bytes.length).longValue());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        storageObjectMetadata.setLastModified(lastModified);
        storageObjectMetadata.setContentLength(contentLength);
        when(cloudVendorService.getObjectMetadata(anyString())).thenReturn(storageObjectMetadata);

        rowGroupData = RowGroupData.builder(rowGroupData)
                .dataValidation(new RowGroupDataValidation("etag", lastModified, contentLength))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        return new TestInitResults(localFile, fillerLength, warmWarmUpElements, rowGroupData);
    }

    @Test
    void test_importWarmUpElements_ok()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-1", "table-1", "s3://test-bucket/column_split_file", 1, 1L, 1, "", "");

        TestInitResults results = testInit(rowGroupKey);

        InputStream inputStream = new FileInputStream(results.localFile);
        inputStream.skip(results.fillerLength);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenReturn(inputStream)
                .thenReturn(InputStream.nullInputStream());

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, results.warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(4, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(0, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_started());
        Assertions.assertEquals(0, warmupImportServiceStats.getimport_elements_failed());
        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_accomplished());

        verify(rowGroupDataService, times(1)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(1)).save(argumentCaptor2.capture());
        RowGroupData savedRowGroupData = argumentCaptor2.getValue();
        Assertions.assertEquals(4, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(0, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        boolean _ = results.localFile.delete();
    }

    @Test
    void test_importWarmUpElements_Exception()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-2", "table-2", "s3://test-bucket/column_split_file", 2, 2L, 2, "", "");

        TestInitResults results = testInit(rowGroupKey);

        InputStream inputStream = new FileInputStream(results.localFile);
        inputStream.skip(results.fillerLength);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenReturn(inputStream)
                .thenThrow(new IllegalArgumentException("test-exception"));

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, results.warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(results.rowGroupData.getWarmUpElements(), returnedRowGroupData.getWarmUpElements());
        Assertions.assertEquals(2, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(2, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_started());
        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_failed());
        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_accomplished());

        verify(rowGroupDataService, times(0)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(0)).save(argumentCaptor2.capture());

        boolean _ = results.localFile.delete();
    }

    @Test
    void test_importWarmUpElements_partial()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-3", "table-3", "s3://test-bucket/column_split_file", 3, 3L, 3, "", "");

        TestInitResults results = testInit(rowGroupKey);

        InputStream inputStream = new FileInputStream(results.localFile);
        inputStream.skip(results.fillerLength);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenReturn(inputStream)
                .thenThrow(new IllegalArgumentException("test-exception"))
                .thenReturn(InputStream.nullInputStream());

        ArgumentCaptor<RowGroupKey> argumentCaptor1 = ArgumentCaptor.forClass(RowGroupKey.class);
        ArgumentCaptor<RowGroupData> argumentCaptor2 = ArgumentCaptor.forClass(RowGroupData.class);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, results.warmWarmUpElements);
        Assertions.assertTrue(optionalRowGroupData.isPresent());

        RowGroupData returnedRowGroupData = optionalRowGroupData.orElseThrow();
        Assertions.assertEquals(3, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(1, returnedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_started());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_elements_failed());
        Assertions.assertEquals(2, warmupImportServiceStats.getimport_elements_accomplished());

        verify(rowGroupDataService, times(1)).flush(argumentCaptor1.capture());
        verify(rowGroupDataService, times(1)).save(argumentCaptor2.capture());
        RowGroupData savedRowGroupData = argumentCaptor2.getValue();
        Assertions.assertEquals(3, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.HOT.equals(warmUpElement.getWarmState())).count());
        Assertions.assertEquals(1, savedRowGroupData.getWarmUpElements().stream().filter(warmUpElement -> WarmState.WARM.equals(warmUpElement.getWarmState())).count());

        boolean _ = results.localFile.delete();
    }

    @Test
    void test_importWarmUpElements_1stFooterValidation()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-4", "table-4", "s3://test-bucket/column_split_file", 4, 4L, 4, "", "");

        TestInitResults results = testInit(rowGroupKey);

        RowGroupData rowGroupData = RowGroupData.builder(results.rowGroupData)
                .dataValidation(new RowGroupDataValidation("etag", results.rowGroupData.getFileModifiedTime(), results.rowGroupData.getFileContentLength() - 1))
                .build();
        when(rowGroupDataService.get(eq(rowGroupKey))).thenReturn(rowGroupData);

        InputStream inputStream = new FileInputStream(results.localFile);
        inputStream.skip(results.fillerLength);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenReturn(inputStream);

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, results.warmWarmUpElements);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(0, warmupImportServiceStats.getimport_elements_started());

        boolean _ = results.localFile.delete();
    }

    @Test
    void test_importWarmUpElements_2ndFooterValidation()
            throws IOException
    {
        RowGroupKey rowGroupKey = new RowGroupKey("schema-5", "table-5", "s3://test-bucket/column_split_file", 5, 5L, 5, "", "");

        TestInitResults results = testInit(rowGroupKey);

        long lastModified = results.rowGroupData.getFileModifiedTime();
        long contentLength = results.rowGroupData.getFileContentLength();

        StorageObjectMetadata storageObjectMetadata1 = new StorageObjectMetadata();
        storageObjectMetadata1.setLastModified(lastModified);
        storageObjectMetadata1.setContentLength(contentLength);
        storageObjectMetadata1.setETag("etag");

        StorageObjectMetadata storageObjectMetadata2 = new StorageObjectMetadata();
        storageObjectMetadata2.setLastModified(lastModified);
        storageObjectMetadata2.setContentLength(contentLength + 1);
        storageObjectMetadata2.setETag("etag");

        when(cloudVendorService.getObjectMetadata(anyString()))
                .thenReturn(storageObjectMetadata1)
                .thenReturn(storageObjectMetadata1)
                .thenReturn(storageObjectMetadata2);

        InputStream inputStream = new FileInputStream(results.localFile);
        inputStream.skip(results.fillerLength);

        when(cloudVendorService.downloadRangeFromCloud(anyString(), anyLong(), anyInt()))
                .thenReturn(inputStream)
                .thenReturn(InputStream.nullInputStream());

        Optional<RowGroupData> optionalRowGroupData = weGroupWarmer.importWarmUpElements(null, rowGroupKey, results.warmWarmUpElements);

        Assertions.assertTrue(optionalRowGroupData.isEmpty());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_elements_started());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_elements_failed());
        Assertions.assertEquals(1, warmupImportServiceStats.getimport_elements_accomplished());

        boolean _ = results.localFile.delete();
    }
}
