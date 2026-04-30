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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.ForIcebergSplitManager;
import io.trino.spi.TrinoException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
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
        ImmutableSet.Builder<DeleteFile> additionalDanglingPositionDeleteFilesBuilder = ImmutableSet.builder();
        for (String dataFilePathWithDV : deleteFilesMetadata.dataFilePathsWithDV()) {
            Set<DeleteFile> positionDeletes = deleteFilesMetadata.dataFilePathsWithPositionDeletes().get(dataFilePathWithDV);
            if (positionDeletes != null) {
                for (DeleteFile positionDelete : positionDeletes) {
                    additionalDanglingPositionDeleteFilesBuilder.add(positionDelete);
                    metrics.danglingPositionDeleteFilesCount.incrementAndGet();
                }
            }
        }
        Set<DeleteFile> danglingDeleteFiles = Sets.union(
                deleteFilesMetadata.danglingDeleteFiles(),
                additionalDanglingPositionDeleteFilesBuilder.build());

        return new DanglingDeleteFilesResult(metrics, danglingDeleteFiles);
    }

    private Set<String> collectDeleteFileReferencedDataFilePaths(BaseTable icebergTable, Snapshot currentSnapshot)
    {
        try {
            return processWithAdditionalThreads(
                    currentSnapshot.deleteManifests(icebergTable.io()).stream()
                            .<Callable<Set<String>>>map(manifest ->
                                    () -> collectDeleteFileReferencedPathsFromManifest(icebergTable, manifest))
                            .collect(toImmutableList()),
                    icebergScanExecutor)
                    .stream()
                    .flatMap(Set::stream)
                    .collect(toImmutableSet());
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process delete manifests for table: " + icebergTable.name(), e);
        }
    }

    private static Set<String> collectDeleteFileReferencedPathsFromManifest(BaseTable icebergTable, ManifestFile manifest)
    {
        ImmutableSet.Builder<String> referencedDataFilePathsBuilder = ImmutableSet.builder();
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                if (contentFile instanceof DeleteFile deleteFile && deleteFile.referencedDataFile() != null) {
                    referencedDataFilePathsBuilder.add(deleteFile.referencedDataFile());
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
        return referencedDataFilePathsBuilder.build();
    }

    private DataFilesMinSequenceNumberMetadata processDataManifests(
            BaseTable icebergTable,
            Snapshot currentSnapshot,
            Set<String> referencedDataFilePaths,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        try {
            return processWithAdditionalThreads(
                    currentSnapshot.dataManifests(icebergTable.io()).stream()
                            .<Callable<DataFilesMinSequenceNumberMetadata>>map(manifest ->
                                    () -> processDataManifestFile(icebergTable, manifest, referencedDataFilePaths, metrics))
                            .collect(toImmutableList()),
                    icebergScanExecutor)
                    .stream()
                    .reduce(DataFilesMinSequenceNumberMetadata::merge)
                    .orElseGet(() -> DataFilesMinSequenceNumberMetadata.builder().build());
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process data manifests for table: " + icebergTable.name(), e);
        }
    }

    private static DataFilesMinSequenceNumberMetadata processDataManifestFile(
            BaseTable icebergTable,
            ManifestFile manifest,
            Set<String> referencedDataFilePaths,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        DataFilesMinSequenceNumberMetadata.Builder builder = DataFilesMinSequenceNumberMetadata.builder();
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                // can return null if data file does not contain sequence number (older iceberg versions)
                if (contentFile.dataSequenceNumber() == null) {
                    metrics.dataFilesWithoutSequenceNumbers.incrementAndGet();
                }
                builder.addDataFile(contentFile, icebergTable, referencedDataFilePaths);
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
        return builder.build();
    }

    private DeleteFilesMetadata processDeleteManifests(
            BaseTable icebergTable,
            Snapshot currentSnapshot,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        try {
            return processWithAdditionalThreads(
                    currentSnapshot.deleteManifests(icebergTable.io()).stream()
                            .<Callable<DeleteFilesMetadata>>map(manifest ->
                                    () -> processDeleteManifestFile(icebergTable, manifest, dataFilesMinSequenceNumberMetadata, metrics))
                            .collect(toImmutableList()),
                    icebergScanExecutor)
                    .stream()
                    .reduce(DeleteFilesMetadata::merge)
                    .orElseGet(() -> DeleteFilesMetadata.builder().build());
        }
        catch (ExecutionException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to process data manifests for table: " + icebergTable.name(), e);
        }
    }

    private static DeleteFilesMetadata processDeleteManifestFile(
            BaseTable icebergTable,
            ManifestFile manifest,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        DeleteFilesMetadata.Builder builder = DeleteFilesMetadata.builder();
        try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, icebergTable);
                CloseableIterator<? extends ContentFile<?>> readerIterator = manifestReader.iterator()) {
            while (readerIterator.hasNext()) {
                ContentFile<?> contentFile = readerIterator.next();
                if (contentFile instanceof DeleteFile deleteFile) {
                    processDeleteFile(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata, builder, metrics);
                }
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path(), e);
        }
        return builder.build();
    }

    private static void processDeleteFile(
            BaseTable icebergTable,
            DeleteFile deleteFile,
            DataFilesMinSequenceNumberMetadata dataFilesMinSequenceNumberMetadata,
            DeleteFilesMetadata.Builder builder,
            DanglingDeleteFilesRemoveMetrics metrics)
    {
        if (deleteFile.format() == FileFormat.PUFFIN) {
            if (isDanglingDeletionVectorDelete(deleteFile, dataFilesMinSequenceNumberMetadata)) {
                metrics.danglingDvFilesCount.incrementAndGet();
                builder.addDanglingDeleteFile(deleteFile);
            }
            else {
                builder.addDataFilePathWithDV(deleteFile.referencedDataFile());
            }
        }
        else if (deleteFile.content() == FileContent.POSITION_DELETES) {
            if (isDanglingPositionDelete(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata, builder)) {
                metrics.danglingPositionDeleteFilesCount.incrementAndGet();
                builder.addDanglingDeleteFile(deleteFile);
            }
        }
        else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
            if (isDanglingEqualityDelete(icebergTable, deleteFile, dataFilesMinSequenceNumberMetadata)) {
                metrics.danglingEqualityDeleteFilesCount.incrementAndGet();
                builder.addDanglingDeleteFile(deleteFile);
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
            DeleteFilesMetadata.Builder builder)
    {
        // Single file-scoped position delete
        String referencedDataFilePath = deleteFile.referencedDataFile();
        if (referencedDataFilePath != null) {
            Long minDataFileSequenceNumber = dataFilesMinSequenceNumberMetadata.minSequenceNumberByReferencedPath().get(referencedDataFilePath);
            if (minDataFileSequenceNumber != null) {
                builder.trackPositionDeleteForDataFile(referencedDataFilePath, deleteFile);
            }
            return minDataFileSequenceNumber == null || !(minDataFileSequenceNumber <= deleteFile.dataSequenceNumber());
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

    private record DataFilesMinSequenceNumberMetadata(
            long globalMinSequenceNumber,
            Map<PartitionKey, Long> minSequenceNumberByPartition,
            Map<String, Long> minSequenceNumberByReferencedPath)
    {
        private DataFilesMinSequenceNumberMetadata
        {
            minSequenceNumberByPartition = ImmutableMap.copyOf(minSequenceNumberByPartition);
            minSequenceNumberByReferencedPath = ImmutableMap.copyOf(minSequenceNumberByReferencedPath);
        }

        private DataFilesMinSequenceNumberMetadata merge(DataFilesMinSequenceNumberMetadata other)
        {
            return new DataFilesMinSequenceNumberMetadata(
                    Math.min(this.globalMinSequenceNumber, other.globalMinSequenceNumber()),
                    Stream.concat(
                                    this.minSequenceNumberByPartition.entrySet().stream(),
                                    other.minSequenceNumberByPartition().entrySet().stream())
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, Math::min)),
                    Stream.concat(
                                    this.minSequenceNumberByReferencedPath.entrySet().stream(),
                                    other.minSequenceNumberByReferencedPath().entrySet().stream())
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, Math::min)));
        }

        private static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private final Map<PartitionKey, Long> minSequenceNumberByPartition;
            private final Map<String, Long> minSequenceNumberByPath;
            private long globalMinSequenceNumber;

            private Builder()
            {
                this.minSequenceNumberByPartition = new HashMap<>();
                this.minSequenceNumberByPath = new HashMap<>();
                this.globalMinSequenceNumber = Long.MAX_VALUE;
            }

            void addDataFile(ContentFile<?> contentFile, BaseTable icebergTable, Set<String> referencedDataFilePaths)
            {
                long dataSequenceNumber = requireNonNullElse(contentFile.dataSequenceNumber(), 0L);
                globalMinSequenceNumber = Math.min(globalMinSequenceNumber, dataSequenceNumber);

                if (referencedDataFilePaths.contains(contentFile.location())) {
                    minSequenceNumberByPath.merge(contentFile.location(), dataSequenceNumber, Math::min);
                }

                PartitionSpec spec = icebergTable.specs().get(contentFile.specId());
                if (!spec.isUnpartitioned()) {
                    PartitionKey key = new PartitionKey(contentFile.specId(), StructLikeWrapper.forType(spec.partitionType()).set(contentFile.partition()));
                    minSequenceNumberByPartition.merge(key, dataSequenceNumber, Math::min);
                }
            }

            DataFilesMinSequenceNumberMetadata build()
            {
                return new DataFilesMinSequenceNumberMetadata(
                        globalMinSequenceNumber,
                        minSequenceNumberByPartition,
                        minSequenceNumberByPath);
            }
        }
    }

    private record DeleteFilesMetadata(
            Set<DeleteFile> danglingDeleteFiles,
            List<String> dataFilePathsWithDV,
            Map<String, Set<DeleteFile>> dataFilePathsWithPositionDeletes)
    {
        private DeleteFilesMetadata
        {
            danglingDeleteFiles = ImmutableSet.copyOf(danglingDeleteFiles);
            dataFilePathsWithDV = ImmutableList.copyOf(dataFilePathsWithDV);
            dataFilePathsWithPositionDeletes = ImmutableMap.copyOf(dataFilePathsWithPositionDeletes);
        }

        private DeleteFilesMetadata merge(DeleteFilesMetadata other)
        {
            return new DeleteFilesMetadata(
                    mergeSets(this.danglingDeleteFiles, other.danglingDeleteFiles()),
                    ImmutableList.<String>builder()
                            .addAll(this.dataFilePathsWithDV)
                            .addAll(other.dataFilePathsWithDV())
                            .build(),
                    Stream.concat(
                                    this.dataFilePathsWithPositionDeletes.entrySet().stream(),
                                    other.dataFilePathsWithPositionDeletes().entrySet().stream())
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, DeleteFilesMetadata::mergeSets)));
        }

        private static Set<DeleteFile> mergeSets(Set<DeleteFile> one, Set<DeleteFile> two)
        {
            return ImmutableSet.<DeleteFile>builder()
                    .addAll(one)
                    .addAll(two)
                    .build();
        }

        private static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private final Set<DeleteFile> danglingDeleteFiles;
            private final List<String> dataFilePathsWithDV;
            private final Map<String, Set<DeleteFile>> dataFilePathsWithPositionDeletes;

            private Builder()
            {
                this.danglingDeleteFiles = new HashSet<>();
                this.dataFilePathsWithDV = new ArrayList<>();
                this.dataFilePathsWithPositionDeletes = new HashMap<>();
            }

            void addDanglingDeleteFile(DeleteFile deleteFile)
            {
                danglingDeleteFiles.add(deleteFile);
            }

            void addDataFilePathWithDV(String path)
            {
                dataFilePathsWithDV.add(path);
            }

            void trackPositionDeleteForDataFile(String referencedDataFilePath, DeleteFile deleteFile)
            {
                dataFilePathsWithPositionDeletes.computeIfAbsent(referencedDataFilePath, _ -> ConcurrentHashMap.newKeySet()).add(deleteFile);
            }

            DeleteFilesMetadata build()
            {
                return new DeleteFilesMetadata(
                        danglingDeleteFiles,
                        dataFilePathsWithDV,
                        dataFilePathsWithPositionDeletes);
            }
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
