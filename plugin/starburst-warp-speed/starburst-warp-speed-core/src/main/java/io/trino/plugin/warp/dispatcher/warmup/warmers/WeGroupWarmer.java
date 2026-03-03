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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.cloudvendors.CloudVendorService;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupDataValidation;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmUtils;
import io.trino.plugin.warp.gen.stats.WarmupImportServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.util.CompressionUtil;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.connector.ConnectorSession;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.gen.errorcodes.ErrorCodes.ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR;
import static java.util.Objects.requireNonNull;

@Singleton
public class WeGroupWarmer
{
    private static final Logger logger = Logger.get(WeGroupWarmer.class);
    private final ShapingLogger shapingLogger;

    private final GlobalConfig globalConfig;
    private final CloudVendorConfig cloudVendorConfig;
    private final StorageEngineConstants storageEngineConstants;
    private final RowGroupDataService rowGroupDataService;
    private final CloudVendorService cloudVendorService;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final JsonMapper jsonMapper;
    private final WarmupImportServiceStats warmupImportServiceStats;

    @Inject
    public WeGroupWarmer(
            GlobalConfig globalConfig,
            @ForWarp CloudVendorConfig cloudVendorConfig,
            StorageEngineConstants storageEngineConstants,
            RowGroupDataService rowGroupDataService,
            @ForWarp CloudVendorService cloudVendorService,
            NativeStorageStateHandler nativeStorageStateHandler,
            JsonMapperProvider jsonMapperProvider,
            MetricsManager metricsManager,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.globalConfig = requireNonNull(globalConfig);
        this.cloudVendorConfig = requireNonNull(cloudVendorConfig);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.jsonMapper = requireNonNull(jsonMapperProvider).get();

        warmupImportServiceStats = metricsManager.registerMetric(new WarmupImportServiceStats());
        shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    @VisibleForTesting
    IsNeedDownloadResults isNeedDownload(RowGroupKey rowGroupKey, String cloudPath)
    {
        RowGroupDataValidation dataValidation = RowGroupDataValidation.EMPTY_VALIDATION;
        boolean isNeedDownload;

        try {
            // check if weGroupFile exist on cloud
            StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(cloudPath);
            isNeedDownload = storageObjectMetadata.getLastModified().isPresent();
            dataValidation = new RowGroupDataValidation(storageObjectMetadata);

            // check if weGroupFile already downloaded
            if (isNeedDownload) {
                RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
                if (rowGroupData != null && rowGroupData.getDataValidation().isValid()) {
                    // footer validation
                    isNeedDownload = !rowGroupData.getDataValidation().equals(dataValidation);
                    if (isNeedDownload) {
                        warmupImportServiceStats.incimport_we_group_footer_validation();
                        logger.debug("isNeedDownload rowGroupKey %s cloudPath '%s' local %s cloud %s",
                                rowGroupKey, cloudPath, rowGroupData.getDataValidation(), dataValidation);
                    }
                }
            }
            logger.debug("rowGroupKey %s cloudPath '%s' => isNeedDownload %s", rowGroupKey, cloudPath, isNeedDownload);
        }
        catch (Exception e) {
            isNeedDownload = false;
            shapingLogger.error("failed to getObjectMetadata rowGroupKey %s cloudPath '%s' message: %s", rowGroupKey, cloudPath, e.getMessage());
        }
        return new IsNeedDownloadResults(isNeedDownload, dataValidation);
    }

    private DownloadResults download(RowGroupKey rowGroupKey, String cloudPath, String localFileName)
    {
        warmupImportServiceStats.incimport_we_group_download_started();
        try {
            FileUtils.createParentDirectories(new File(localFileName));
        }
        catch (Throwable e) {
            nativeStorageStateHandler.handleErrorCode(ENV_EXCEPTION_STORAGE_TEMPORARY_ERROR);
            shapingLogger.warn(e,
                    "storage temporary disabled since failed to create directories for %s", localFileName);
            return new DownloadResults();
        }

        try {
            File localFile = new File(localFileName);
            StorageObjectMetadata metadata = cloudVendorService.downloadFileFromCloud(cloudPath, localFile);

            if (metadata.getContentLength().isPresent() && (localFile.length() != metadata.getContentLength().get())) {
                warmupImportServiceStats.incimport_we_group_download_failed();
                shapingLogger.error("failed to download weGroupFile rowGroupKey %s cloudPath '%s' localFile.length %d != metadata.contentLength %d",
                        rowGroupKey, cloudPath, localFile.length(), metadata.getContentLength().get());
                // delete localFile
                FileUtils.deleteQuietly(localFile);
                return new DownloadResults();
            }
            return new DownloadResults(true, new RowGroupDataValidation(metadata));
        }
        catch (Exception e) {
            warmupImportServiceStats.incimport_we_group_download_failed();
            shapingLogger.error("failed to download weGroupFile rowGroupKey %s cloudPath '%s' exception: %s cause: %s",
                    rowGroupKey, cloudPath, e, e.getCause());
            return new DownloadResults();
        }
        finally {
            warmupImportServiceStats.incimport_we_group_download_accomplished();
        }
    }

    private void copyFile(String sourceFileName, String targetFileName)
    {
        try {
            Files.copy(Path.of(sourceFileName), Path.of(targetFileName));
        }
        catch (IOException e) {
            throw new RuntimeException("Files.copy %s to %s failed exception: %s cause: %s"
                    .formatted(sourceFileName, targetFileName, e, e.getCause()), e);
        }
    }

    private RowGroupData refreshRowGroupDataAfterImport(RowGroupKey rowGroupKey, String localTmpFileName, String localFileName, RowGroupDataValidation dataValidation)
    {
        RowGroupData rowGroupData = null;
        boolean locked = false;
        String localSaveFileName = null;
        boolean localSaveFileExist = false;

        // take writeLock
        try {
            rowGroupData = rowGroupDataService.get(rowGroupKey);

            if (rowGroupData != null) {
                rowGroupData.getLock().writeLock();
                locked = true;

                // save local file
                localSaveFileName = localFileName + ".save";
                copyFile(localFileName, localSaveFileName);
                localSaveFileExist = true;

                // delete local file and invalidate cache
                rowGroupDataService.deleteData(rowGroupData, true);
            }

            // rename
            File localTmpFile = new File(localTmpFileName);
            if (!localTmpFile.renameTo(new File(localFileName))) {
                // delete localTmpFile
                FileUtils.deleteQuietly(localTmpFile);
                throw new RuntimeException("failed renaming file " + localTmpFileName + " => " + localFileName);
            }

            // reload
            RowGroupData newRowGroupData = rowGroupDataService.reload(rowGroupKey, rowGroupData);

            // update
            if (newRowGroupData != null) {
                List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();

                for (WarmUpElement existingWarmUpElement : newRowGroupData.getWarmUpElements()) {
                    updatedWarmUpElements.add(WarmUpElement.builder(existingWarmUpElement).isImported(true).build());
                }
                rowGroupData = RowGroupData.builder(newRowGroupData)
                        .warmUpElements(updatedWarmUpElements)
                        .nextExportOffset(newRowGroupData.getNextOffset())
                        .dataValidation(dataValidation)
                        .build();
                rowGroupDataService.save(rowGroupData);
            }
            else {
                // ToDo: import_we_group_download_corrupted_file
                shapingLogger.error("failed to get RowGroupData for row group %s", rowGroupKey);
                if (localSaveFileExist) {
                    // restore local file and cache
                    copyFile(localSaveFileName, localFileName);
                    rowGroupDataService.save(rowGroupData);
                }
            }
            return rowGroupData;
        }
        catch (InterruptedException e) {
            shapingLogger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
            return null;
        }
        catch (Exception e) {
            // ToDo: import_we_group_download_corrupted_file
            shapingLogger.error("failed to get RowGroupData for row group %s exception: %s cause: %s", rowGroupKey, e, e.getCause());
            if (localSaveFileExist) {
                // delete corrupted local file
                File localFile = new File(localFileName);
                if (localFile.exists()) {
                    //noinspection ResultOfMethodCallIgnored
                    localFile.delete();
                }
                // restore local file and cache
                copyFile(localSaveFileName, localFileName);
                rowGroupDataService.save(rowGroupData);
            }
            return rowGroupData;
        }
        finally {
            if (localSaveFileExist) {
                // delete saved file
                File localSaveFile = new File(localSaveFileName);
                if (localSaveFile.exists()) {
                    //noinspection ResultOfMethodCallIgnored
                    localSaveFile.delete();
                }
            }
            if (rowGroupData != null && locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
    }

    public Optional<RowGroupData> importWeGroup(ConnectorSession session, RowGroupKey rowGroupKey)
    {
        String cloudImportExportPath = WarpSessionProperties.getS3ImportExportPath(session, cloudVendorConfig, cloudVendorService);
        String cloudPath = WarmUtils.getCloudPath(rowGroupKey, cloudImportExportPath);
        IsNeedDownloadResults isNeedDownloadResults;
        StopWatch stopWatch = new StopWatch();

        logger.debug("importWeGroup rowGroupKey %s cloudPath '%s'", rowGroupKey, cloudPath);
        try {
            stopWatch.start();
            warmupImportServiceStats.incimport_row_group_count_started();

            isNeedDownloadResults = isNeedDownload(rowGroupKey, cloudPath);
            if (!isNeedDownloadResults.isNeedDownload) {
                return Optional.empty();
            }

            String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
            String localTmpFileName = localFileName + ".tmp";
            DownloadResults downloadResults = download(rowGroupKey, cloudPath, localTmpFileName);

            if (!downloadResults.isDownloadDone) {
                warmupImportServiceStats.incimport_row_group_count_failed();
                return Optional.empty();
            }

            // at this point lastModified must be present, if it's not it's a bug
            RowGroupData rowGroupData = refreshRowGroupDataAfterImport(rowGroupKey, localTmpFileName, localFileName,
                    downloadResults.dataValidation.isValid() ? downloadResults.dataValidation : isNeedDownloadResults.dataValidation);
            if (rowGroupData == null) {
                warmupImportServiceStats.incimport_row_group_count_failed();
                return Optional.empty();
            }
            return Optional.of(rowGroupData);
        }
        finally {
            stopWatch.stop();
            warmupImportServiceStats.addimport_row_group_total_time(stopWatch.getNanoTime());
            warmupImportServiceStats.incimport_row_group_count_accomplished();
        }
    }

    private RowGroupDataValidation getRowGroupDataValidation(String cloudPath)
    {
        StorageObjectMetadata storageObjectMetadata = cloudVendorService.getObjectMetadata(cloudPath);
        return new RowGroupDataValidation(storageObjectMetadata);
    }

    private boolean validate(RowGroupData rowGroupData, String cloudPath)
    {
        RowGroupDataValidation dataValidation = getRowGroupDataValidation(cloudPath);

        if (!rowGroupData.getDataValidation().equals(dataValidation)) {
            shapingLogger.error("validate failed. cloud file changed. rowGroupKey %s dataValidation local %s != cloud %s",
                    rowGroupData.getRowGroupKey(), rowGroupData.getDataValidation(), dataValidation);
            return false;
        }

        long startOffset = (long) rowGroupData.getNextExportOffset() * storageEngineConstants.getPageSize();
        int totalLength = Long.valueOf(dataValidation.fileContentLength() - startOffset).intValue();

        try (InputStream inputStream = cloudVendorService.downloadRangeFromCloud(cloudPath, startOffset, totalLength)) {
            byte[] buffer = new byte[totalLength];
            int readBytes = inputStream.read(buffer);

            if (readBytes != totalLength) {
                shapingLogger.error("validate failed. read cloud footer failed. rowGroupKey %s", rowGroupData.getRowGroupKey());
                return false;
            }

            String str = CompressionUtil.decompressGzip(buffer);
            RowGroupData cloudRowGroupData = jsonMapper.readerFor(RowGroupData.class).readValue(str);

            List<WarmUpElement> warmUpElements = (List<WarmUpElement>) rowGroupData.getWarmUpElements();
            List<WarmUpElement> cloudWarmUpElements = (List<WarmUpElement>) cloudRowGroupData.getWarmUpElements();

            if (warmUpElements.size() != cloudWarmUpElements.size()) {
                shapingLogger.error("validate failed. warmUpElements.size not equal. rowGroupKey %s local %d != cloud %d",
                        rowGroupData.getRowGroupKey(), warmUpElements.size(), cloudWarmUpElements.size());
                return false;
            }

            for (int i = 0; i < warmUpElements.size(); i++) {
                WarmUpElement warmUpElement = warmUpElements.get(i);
                WarmUpElement cloudWarmUpElement = cloudWarmUpElements.get(i);

                if ((warmUpElement.getStartOffset() != cloudWarmUpElement.getStartOffset()) ||
                        (warmUpElement.getEndOffset() != cloudWarmUpElement.getEndOffset())) {
                    shapingLogger.error("validate failed. warmUpElement offsets not equal. rowGroupKey %s local %d-%d != cloud %d-%d",
                            rowGroupData.getRowGroupKey(), warmUpElement.getStartOffset(), warmUpElement.getEndOffset(),
                            cloudWarmUpElement.getStartOffset(), cloudWarmUpElement.getEndOffset());
                    return false;
                }
            }

            return true;
        }
        catch (Exception e) {
            shapingLogger.error("failed to download footer rowGroupKey %s cloudPath '%s' exception: %s cause: %s",
                    rowGroupData.getRowGroupKey(), cloudPath, e, e.getCause());
            return false;
        }
    }

    private DownloadRangeResults download(RowGroupData rowGroupData, WarmUpElement warmUpElement, String cloudPath, String localFileName)
    {
        // 1st footer validation
        RowGroupDataValidation dataValidation = getRowGroupDataValidation(cloudPath);
        boolean isValidationOk = rowGroupData.getDataValidation().equals(dataValidation);
        boolean isDownloadDone = false;

        if (isValidationOk) {
            long startOffset = (long) warmUpElement.getStartOffset() * storageEngineConstants.getPageSize();
            int totalLength = (warmUpElement.getEndOffset() - warmUpElement.getStartOffset()) * storageEngineConstants.getPageSize();

            warmupImportServiceStats.incimport_elements_started();
            try (InputStream inputStream = cloudVendorService.downloadRangeFromCloud(cloudPath, startOffset, totalLength);
                    RandomAccessFile outputRandomAccessFile = new RandomAccessFile(localFileName, "rw")) {
                byte[] buffer = new byte[Math.min(totalLength, 8192 * 100)]; // PageSize * 100
                int length;

                outputRandomAccessFile.seek(startOffset);
                while ((length = inputStream.read(buffer)) > 0) {
                    outputRandomAccessFile.write(buffer, 0, length);
                }
                isDownloadDone = true;

                // 2nd footer validation
                dataValidation = getRowGroupDataValidation(cloudPath);
                isValidationOk = rowGroupData.getDataValidation().equals(dataValidation);

                if (!isValidationOk) {
                    warmupImportServiceStats.incimport_elements_failed();
                    warmupImportServiceStats.incimport_element_2nd_footer_validation();
                    logger.debug("download (2nd footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                            rowGroupData.getRowGroupKey(), cloudPath, rowGroupData.getDataValidation(), dataValidation);
                }
            }
            catch (Exception e) {
                warmupImportServiceStats.incimport_elements_failed();
                shapingLogger.error("failed to download warmUpElement rowGroupKey %s cloudPath '%s' exception: %s cause: %s",
                        rowGroupData.getRowGroupKey(), cloudPath, e, e.getCause());
            }
            finally {
                warmupImportServiceStats.incimport_elements_accomplished();
            }
        }
        else {
            warmupImportServiceStats.incimport_element_1st_footer_validation();
            logger.debug("download (1st footer validation) rowGroupKey %s cloudPath '%s' local %s cloud %s",
                    rowGroupData.getRowGroupKey(), cloudPath, rowGroupData.getDataValidation(), dataValidation);
        }
        return new DownloadRangeResults(isValidationOk, isDownloadDone);
    }

    public Optional<RowGroupData> importWarmUpElements(ConnectorSession session, RowGroupKey rowGroupKey, List<WarmUpElement> warmWarmUpElements)
    {
        String cloudImportExportPath = WarpSessionProperties.getS3ImportExportPath(session, cloudVendorConfig, cloudVendorService);
        String cloudPath = WarmUtils.getCloudPath(rowGroupKey, cloudImportExportPath);
        String localFileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        boolean locked = false;

        logger.debug("importWarmUpElements rowGroupKey %s cloudPath '%s' localFileName '%s' warmWarmUpElements size %d",
                rowGroupKey, cloudPath, localFileName, warmWarmUpElements.size());
        // take writeLock
        try {
            rowGroupData.getLock().writeLock();
            locked = true;

            if (!validate(rowGroupData, cloudPath)) {
                rowGroupDataService.deleteData(rowGroupData, true);
                return Optional.empty();
            }

            List<WarmUpElement> warmUpElements = new ArrayList<>(rowGroupData.getWarmUpElements());
            boolean isDownloadDone = false;

            for (WarmUpElement warmWarmUpElement : warmWarmUpElements) {
                DownloadRangeResults downloadResults = download(rowGroupData, warmWarmUpElement, cloudPath, localFileName);

                if (!downloadResults.isValidationOk) {
                    rowGroupDataService.deleteData(rowGroupData, true);
                    return Optional.empty();
                }

                if (downloadResults.isDownloadDone) {
                    WarmUpElement warmUpElement = WarmUpElement.builder(warmWarmUpElement)
                            .isImported(true)
                            .warmState(WarmState.HOT)
                            .build();

                    Optional<WarmUpElement> weToOverride = warmUpElements.stream().filter(warmWarmUpElement::isRepresentTheSameElement).findFirst();
                    weToOverride.ifPresent(warmUpElements::remove);
                    warmUpElements.add(warmUpElement);
                    isDownloadDone = true;
                }
            }

            if (isDownloadDone) {
                rowGroupData = RowGroupData.builder(rowGroupData)
                        .warmUpElements(warmUpElements)
                        .build();
                rowGroupDataService.save(rowGroupData);
                rowGroupDataService.flush(rowGroupKey);
            }
        }
        catch (InterruptedException e) {
            shapingLogger.warn(e, "failed to acquire write lock for row group %s", rowGroupKey);
        }
        finally {
            if (locked) {
                rowGroupData.getLock().writeUnlock();
            }
        }
        return Optional.of(rowGroupData);
    }

    @VisibleForTesting
    record IsNeedDownloadResults(boolean isNeedDownload, RowGroupDataValidation dataValidation)
    {
    }

    private record DownloadResults(boolean isDownloadDone, RowGroupDataValidation dataValidation)
    {
        DownloadResults()
        {
            this(false, RowGroupDataValidation.EMPTY_VALIDATION);
        }
    }

    private record DownloadRangeResults(boolean isValidationOk, boolean isDownloadDone)
    {
    }
}
