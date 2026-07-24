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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.spi.TrinoException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class RemoveDanglingDeleteFiles
{
    private static final Logger log = Logger.get(RemoveDanglingDeleteFiles.class);

    private final ExecutorService icebergScanExecutor;

    @Inject
    public RemoveDanglingDeleteFiles(@ForIcebergSplitManager ExecutorService icebergScanExecutor)
    {
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
    }

    /**
     * This method performs scanning delete manifests two times, and scanning data manifests one time.
     * It could be reduced to scanning both one time, with an additional memory consumption due to collecting all data file paths.
     * It has been decided that doing one additional scan is preferred over consuming excessive memory.
     */
    public DanglingDeleteFilesResult collectDanglingDeleteFiles(BaseTable icebergTable)
    {
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            return DanglingDeleteFilesResult.EMPTY;
        }

        Set<String> referencedDataFilePaths = collectDeleteFileReferencedDataFilePaths(icebergTable, currentSnapshot);
        DanglingDeleteFilesRemoveMetrics metrics = new DanglingDeleteFilesRemoveMetrics();

        DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata = processDataManifests(
                icebergTable,
                currentSnapshot,
                referencedDataFilePaths,
                metrics);

        DeleteFilesMetadata deleteFilesMetadata = processDeleteManifests(
                icebergTable,
                currentSnapshot,
                dataFilesMinSequenceNumberMetadata,
                metrics);

        // Mark file-scoped position deletes as dangling if their referenced data file is covered by an active DV
        // (per spec: position deletes do not apply when a deletion vector exists for the same data file)
        for (String dataFilePathWithDV : deleteFilesMetadata.dataFilePathsWithDV()) {
            Set<DeleteFile> positionDeletes = deleteFilesMetadata.dataFilePathsWithPositionDeletes().get(dataFilePathWithDV);
            if (positionDeletes != null) {
                for (DeleteFile positionDelete : positionDeletes) {
                    deleteFilesMetadata.addDanglingDeleteFile(positionDelete);
                    metrics.danglingPositionDeleteFilesCount.incrementAndGet();
                }
            }
        }

        return new DanglingDeleteFilesResult(metrics, deleteFilesMetadata.danglingDeleteFiles());
    }

    private Set<String> collectDeleteFileReferencedDataFilePaths(BaseTable icebergTable, Snapshot currentSnapshot)
    {
        Set<String> referencedDataFilePaths = ConcurrentHashMap.newKeySet();
        try {
            processWithAdditionalThreads(
                    currentSnapshot.deleteManifests(icebergTable.io()).stream()
                            .<Callable<Void>>map(manifest ->
                                    () -> {
                                        collectDeleteFileReferencedPathsFromManifest(icebergTable, manifest, referencedDataFilePaths);
                                        return null;
                                    })
                            .collect(toImmutableList()),
                    icebergScanExecutor);
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process delete manifests for table: " + icebergTable.name(), e);
        }
        return referencedDataFilePaths;
    }

    private static void collectDeleteFileReferencedPathsFromManifest(BaseTable icebergTable, ManifestFile manifest, Set<String> referencedDataFilePaths)
    {
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                if (contentFile instanceof DeleteFile deleteFile) {
                    String referencedDataFile = ContentFileUtil.referencedDataFileLocation(deleteFile);
                    if (referencedDataFile != null) {
                        referencedDataFilePaths.add(referencedDataFile);
                    }
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
    }

    private DataFilesMinSequenceNumberMetadata processDataManifests(
            BaseTable icebergTable,
            Snapshot currentSnapshot,
            Set<String> referencedDataFilePaths,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        DataFilesMinSequenceNumberMetadata metadata = new DataFilesMinSequenceNumberMetadata();
        try {
            processWithAdditionalThreads(
                    currentSnapshot.dataManifests(icebergTable.io()).stream()
                            .<Callable<Void>>map(manifest ->
                                    () -> {
                                        processDataManifestFile(icebergTable, manifest, referencedDataFilePaths, metadata, metrics);
                                        return null;
                                    })
                            .collect(toImmutableList()),
                    icebergScanExecutor);
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process data manifests for table: " + icebergTable.name(), e);
        }
        return metadata;
    }

    private static void processDataManifestFile(
            BaseTable icebergTable,
            ManifestFile manifest,
            Set<String> referencedDataFilePaths,
            DataFilesMinSequenceNumberMetadata metadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                // can return null if data file does not contain sequence number (older iceberg versions)
                if (contentFile.dataSequenceNumber() == null) {
                    metrics.dataFilesWithoutSequenceNumbers.incrementAndGet();
                }
                metadata.addDataFile(contentFile, icebergTable, referencedDataFilePaths);
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
    }

    private DeleteFilesMetadata processDeleteManifests(
            BaseTable icebergTable,
            Snapshot currentSnapshot,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        DeleteFilesMetadata metadata = new DeleteFilesMetadata();
        try {
            processWithAdditionalThreads(
                    currentSnapshot.deleteManifests(icebergTable.io()).stream()
                            .<Callable<Void>>map(manifest ->
                                    () -> {
                                        processDeleteManifestFile(icebergTable, manifest, dataFilesMinSequenceNumberMetadata, metadata, metrics);
                                        return null;
                                    })
                            .collect(toImmutableList()),
                    icebergScanExecutor);
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process data manifests for table: " + icebergTable.name(), e);
        }
        return metadata;
    }

    private static void processDeleteManifestFile(
            BaseTable icebergTable,
            ManifestFile manifest,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DeleteFilesMetadata metadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                if (contentFile instanceof DeleteFile deleteFile) {
                    processDeleteFile(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata, metadata, metrics);
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
    }

    private static void processDeleteFile(
            BaseTable icebergTable,
            DeleteFile deleteFile,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DeleteFilesMetadata metadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        if (deleteFile.content() == FileContent.POSITION_DELETES) {
            if (ContentFileUtil.isDV(deleteFile)) {
                if (isDanglingDeletionVectorDelete(deleteFile, dataFilesMinSequenceNumberMetadata)) {
                    metrics.danglingDvFilesCount.incrementAndGet();
                    metadata.addDanglingDeleteFile(deleteFile);
                }
                else {
                    metadata.addDataFilePathWithDV(deleteFile.referencedDataFile());
                }
            }
            else if (isDanglingPositionDelete(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata, metadata)) {
                metrics.danglingPositionDeleteFilesCount.incrementAndGet();
                metadata.addDanglingDeleteFile(deleteFile);
            }
        }
        else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
            if (isDanglingEqualityDelete(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata)) {
                metrics.danglingEqualityDeleteFilesCount.incrementAndGet();
                metadata.addDanglingDeleteFile(deleteFile);
            }
        }
        else {
            metrics.unexpectedDeleteFilesCount.incrementAndGet();
        }
    }

    /**
     * Per the spec, deletion vector deletes apply when: The data file's data sequence number is less than or equal to the deletion vector's data sequence number
     * See <a href="https://iceberg.apache.org/spec/#scan-planning"/>
     */
    private static boolean isDanglingDeletionVectorDelete(DeleteFile deleteFile, DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata)
    {
        String referencedDataFilePath = deleteFile.referencedDataFile();
        if (referencedDataFilePath == null) {
            log.warn("Encountered deletion vector delete file without reference data file at: " + deleteFile.path());
            return true; // No referenced file — dangling
        }
        Long minDataFileSequenceNumber = dataFilesMinSequenceNumberMetadata.minSequenceNumberByReferencedPath().get(referencedDataFilePath);
        return minDataFileSequenceNumber == null || !(minDataFileSequenceNumber <= deleteFile.dataSequenceNumber());
    }

    /**
     * Per the spec, position deletes apply when: The data file's data sequence number is less than or equal to the delete file's data sequence number
     * See <a href="https://iceberg.apache.org/spec/#scan-planning"/>
     */
    private static boolean isDanglingPositionDelete(
            BaseTable icebergTable,
            DeleteFile deleteFile,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DeleteFilesMetadata metadata)
    {
        // Single file-scoped position delete (either via the explicit referenced_data_file pointer
        // or when the delete file's _file column metric's lower/upper bounds are equal)
        String referencedDataFilePath = ContentFileUtil.referencedDataFileLocation(deleteFile);
        if (referencedDataFilePath != null) {
            Long minDataFileSequenceNumber = dataFilesMinSequenceNumberMetadata.minSequenceNumberByReferencedPath().get(referencedDataFilePath);
            boolean dangling = minDataFileSequenceNumber == null || !(minDataFileSequenceNumber <= deleteFile.dataSequenceNumber());
            if (!dangling) {
                metadata.trackPositionDeleteForDataFile(referencedDataFilePath, deleteFile);
            }
            return dangling;
        }
        PartitionSpec spec = icebergTable.specs().get(deleteFile.specId());
        // Non-partition scoped position delete
        if (spec.isUnpartitioned()) {
            return !(dataFilesMinSequenceNumberMetadata.globalMinSequenceNumber() <= deleteFile.dataSequenceNumber());
        }
        // Partition-scoped position delete
        PartitionKey key = new PartitionKey(deleteFile.specId(), StructLikeWrapper.forType(spec.partitionType()).set(deleteFile.partition()));
        Long minPartitionSequenceNumber = dataFilesMinSequenceNumberMetadata.minSequenceNumberByPartition().get(key);
        return minPartitionSequenceNumber == null || !(minPartitionSequenceNumber <= deleteFile.dataSequenceNumber());
    }

    /**
     * Per the spec, equality deletes apply when: The data file's data sequence number is strictly less than the delete's data sequence number
     * See <a href="https://iceberg.apache.org/spec/#scan-planning"/>
     */
    private static boolean isDanglingEqualityDelete(
            BaseTable icebergTable,
            DeleteFile deleteFile,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata)
    {
        PartitionSpec spec = icebergTable.specs().get(deleteFile.specId());
        // Global equality delete
        if (spec.isUnpartitioned()) {
            return !(dataFilesMinSequenceNumberMetadata.globalMinSequenceNumber() < deleteFile.dataSequenceNumber());
        }
        PartitionKey key = new PartitionKey(deleteFile.specId(), StructLikeWrapper.forType(spec.partitionType()).set(deleteFile.partition()));
        Long minPartitionSequenceNumber = dataFilesMinSequenceNumberMetadata.minSequenceNumberByPartition().get(key);
        // Partition-scoped equality delete
        return minPartitionSequenceNumber == null || !(minPartitionSequenceNumber < deleteFile.dataSequenceNumber());
    }

    /**
     * Concurrent threadsafe accumulator populated directly by manifest-scan threads.
     */
    @ThreadSafe
    private static final class DataFilesMinSequenceNumberMetadata
    {
        private final Map<PartitionKey, Long> minSequenceNumberByPartition = new ConcurrentHashMap<>();
        private final Map<String, Long> minSequenceNumberByReferencedPath = new ConcurrentHashMap<>();
        private final LongAccumulator globalMinSequenceNumber = new LongAccumulator(Math::min, Long.MAX_VALUE);

        void addDataFile(ContentFile<?> contentFile, BaseTable icebergTable, Set<String> referencedDataFilePaths)
        {
            long dataSequenceNumber = requireNonNullElse(contentFile.dataSequenceNumber(), 0L);
            globalMinSequenceNumber.accumulate(dataSequenceNumber);

            if (referencedDataFilePaths.contains(contentFile.location())) {
                minSequenceNumberByReferencedPath.merge(contentFile.location(), dataSequenceNumber, Math::min);
            }

            PartitionSpec spec = icebergTable.specs().get(contentFile.specId());
            if (!spec.isUnpartitioned()) {
                PartitionKey key = new PartitionKey(contentFile.specId(), StructLikeWrapper.forType(spec.partitionType()).set(contentFile.partition()));
                minSequenceNumberByPartition.merge(key, dataSequenceNumber, Math::min);
            }
        }

        long globalMinSequenceNumber()
        {
            return globalMinSequenceNumber.get();
        }

        Map<PartitionKey, Long> minSequenceNumberByPartition()
        {
            return minSequenceNumberByPartition;
        }

        Map<String, Long> minSequenceNumberByReferencedPath()
        {
            return minSequenceNumberByReferencedPath;
        }
    }

    /**
     * Concurrent threadsafe accumulator populated directly by delete-manifest-scan threads.
     */
    @ThreadSafe
    private static final class DeleteFilesMetadata
    {
        private final Set<DeleteFile> danglingDeleteFiles = ConcurrentHashMap.newKeySet();
        private final Set<String> dataFilePathsWithDV = ConcurrentHashMap.newKeySet();
        private final Map<String, Set<DeleteFile>> dataFilePathsWithPositionDeletes = new ConcurrentHashMap<>();

        void addDanglingDeleteFile(DeleteFile deleteFile)
        {
            danglingDeleteFiles.add(deleteFile.copyWithoutStats());
        }

        void addDataFilePathWithDV(String path)
        {
            dataFilePathsWithDV.add(path);
        }

        void trackPositionDeleteForDataFile(String referencedDataFilePath, DeleteFile deleteFile)
        {
            dataFilePathsWithPositionDeletes.computeIfAbsent(referencedDataFilePath, _ -> ConcurrentHashMap.newKeySet()).add(deleteFile.copyWithoutStats());
        }

        Set<DeleteFile> danglingDeleteFiles()
        {
            return Collections.unmodifiableSet(danglingDeleteFiles);
        }

        Set<String> dataFilePathsWithDV()
        {
            return Collections.unmodifiableSet(dataFilePathsWithDV);
        }

        Map<String, Set<DeleteFile>> dataFilePathsWithPositionDeletes()
        {
            return Collections.unmodifiableMap(dataFilePathsWithPositionDeletes);
        }
    }

    public static class DanglingDeleteFilesRemoveMetrics
    {
        private final AtomicLong danglingEqualityDeleteFilesCount = new AtomicLong();
        private final AtomicLong danglingPositionDeleteFilesCount = new AtomicLong();
        private final AtomicLong danglingDvFilesCount = new AtomicLong();
        private final AtomicLong dataFilesWithoutSequenceNumbers = new AtomicLong();
        private final AtomicLong unexpectedDeleteFilesCount = new AtomicLong();

        public long getDanglingEqualityDeleteFilesCount()
        {
            return danglingEqualityDeleteFilesCount.get();
        }

        public long getDanglingPositionDeleteFilesCount()
        {
            return danglingPositionDeleteFilesCount.get();
        }

        public long getDanglingDvFilesCount()
        {
            return danglingDvFilesCount.get();
        }

        public long getDataFilesWithoutSequenceNumbers()
        {
            return dataFilesWithoutSequenceNumbers.get();
        }

        public long getUnexpectedDeleteFilesCount()
        {
            return unexpectedDeleteFilesCount.get();
        }
    }

    public record DanglingDeleteFilesResult(
            DanglingDeleteFilesRemoveMetrics metrics,
            Set<DeleteFile> danglingDeleteFiles)
    {
        private static final DanglingDeleteFilesResult EMPTY = new DanglingDeleteFilesResult(
                new DanglingDeleteFilesRemoveMetrics(),
                ImmutableSet.of());
    }

    private record PartitionKey(int specId, StructLikeWrapper partition) {}
}
