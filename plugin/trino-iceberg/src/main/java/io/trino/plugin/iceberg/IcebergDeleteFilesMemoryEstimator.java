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
package io.trino.plugin.iceberg;

import com.google.common.primitives.Longs;
import io.airlift.log.Logger;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.math.LongMath.saturatedMultiply;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static java.lang.Math.max;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_EQ_DELETES_PROP;

public final class IcebergDeleteFilesMemoryEstimator
{
    /**
     * Fixed part of an indexed equality delete file (object graph and path strings, measured
     * ~2.5kB) and the marginal cost per equality column carrying bounds. The measured marginal
     * grows with width (~250B for narrow schemas, ~420B in the 10-20 column range), so the bound
     * holds at the formula level — the base's surplus absorbs the tail — and
     * {@code TestDeleteFileIndexMemory} asserts the combined formula covers each measured
     * scenario, not each constant separately.
     */
    public static final long EQUALITY_DELETE_FILE_BASE_MEMORY = 3 * 1024;
    public static final long EQUALITY_DELETE_FILE_MEMORY_PER_COLUMN = 384;

    private static final Logger log = Logger.get(IcebergDeleteFilesMemoryEstimator.class);

    private IcebergDeleteFilesMemoryEstimator() {}

    /**
     * A model of the {@code DeleteFileIndex} the scan will build: delete files that survive
     * partition-filter pruning of the delete manifests (the same pruning the index itself applies)
     * times a per-file cost that depends on whether the snapshot carries equality deletes, whose
     * column bounds make an indexed file several times heavier than a positional one. Residual row
     * filtering prunes further, so the count side stays an upper bound.
     */
    public static long estimateSplitSourceMemory(
            Table table,
            IcebergTableHandle tableHandle,
            Scan<?, FileScanTask, CombinedScanTask> tableScan,
            long memoryPerPositionalDeleteFile,
            long memoryPerEqualityDeleteFile)
    {
        if (tableHandle.getSnapshotId().isEmpty()) {
            return 0;
        }
        Snapshot snapshot = table.snapshot(tableHandle.getSnapshotId().getAsLong());
        if (snapshot == null) {
            return 0;
        }
        long deleteFileCount = estimateDeleteFileCount(table, tableHandle, snapshot, tableScan);
        if (deleteFileCount == 0) {
            return 0;
        }
        if (hasEqualityDeletes(snapshot)) {
            long memoryPerEqualityFile = equalityDeleteFileMemory(table.schema().identifierFieldIds().size(), memoryPerEqualityDeleteFile);
            return saturatedMultiply(deleteFileCount, memoryPerEqualityFile);
        }
        return saturatedMultiply(deleteFileCount, memoryPerPositionalDeleteFile);
    }

    /**
     * Memory per indexed equality delete file: the configured default is a floor calibrated for
     * narrow identifier schemas, and the per-column formula grows it for wide ones — each equality
     * column carries bounds worth roughly {@link #EQUALITY_DELETE_FILE_MEMORY_PER_COLUMN} bytes, so
     * past ~13 columns a flat default under-reserves. The formula only ever raises the estimate:
     * writers may use equality columns beyond the declared identifier fields, so a computed value
     * below the calibrated default is not trusted.
     */
    public static long equalityDeleteFileMemory(int identifierColumnCount, long configuredDefault)
    {
        return max(configuredDefault, EQUALITY_DELETE_FILE_BASE_MEMORY + EQUALITY_DELETE_FILE_MEMORY_PER_COLUMN * identifierColumnCount);
    }

    /**
     * The summary carries row counts, not file counts, but a zero row count still implies no
     * equality delete files exist (short of a writer committing an empty delete file). A missing
     * or unreadable summary is treated as having them, so the estimate errs high.
     */
    private static boolean hasEqualityDeletes(Snapshot snapshot)
    {
        if (snapshot.summary() == null) {
            return true;
        }
        String totalEqualityDeletes = snapshot.summary().get(TOTAL_EQ_DELETES_PROP);
        if (totalEqualityDeletes == null) {
            return true;
        }
        Long count = Longs.tryParse(totalEqualityDeletes);
        return count == null || count > 0;
    }

    /**
     * Counts delete files in the delete manifests that survive the same partition-filter pruning
     * {@code DeleteFileIndex} applies when the scan plans, from manifest-list metadata alone. The
     * manifest list is memoized by the snapshot, so the scan's own planning reuses this read.
     * Falls back to the snapshot summary's whole-table count when the list is unreadable or a
     * manifest lacks file counts.
     */
    private static long estimateDeleteFileCount(Table table, IcebergTableHandle tableHandle, Snapshot snapshot, Scan<?, FileScanTask, CombinedScanTask> tableScan)
    {
        // The estimate is best-effort and must never fail the task: any failure while reading the
        // manifest list or pruning (evaluator and projection included) falls back to the summary's
        // whole-table count, which errs high when the summary is present. With no readable summary
        // either, the fallback yields zero — the one corner that errs low, since there is nothing
        // left to base a reservation on.
        try {
            return prunedDeleteFileCount(table, tableHandle, snapshot, tableScan);
        }
        catch (RuntimeException e) {
            log.debug(e, "Failed to count pruned delete manifests of table %s, falling back to the summary total", table.name());
            return summaryDeleteFileCount(snapshot);
        }
    }

    private static long prunedDeleteFileCount(Table table, IcebergTableHandle tableHandle, Snapshot snapshot, Scan<?, FileScanTask, CombinedScanTask> tableScan)
    {
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(table.io());
        // The scan is created unfiltered — IcebergSplitSource applies the predicate to a copy at
        // planning time — so the filter is rebuilt here the same way, from the handle's predicates.
        // Dynamic filters are not known yet at estimation time and are left out.
        Expression dataFilter = toIcebergExpression(
                tableHandle.getEnforcedPredicate()
                        .filter((column, _) -> !isMetadataColumnId(column.getId()))
                        .intersect(tableHandle.getUnenforcedPredicate()));
        boolean caseSensitive = tableScan.isCaseSensitive();
        Map<Integer, ManifestEvaluator> evaluators = new HashMap<>();
        long deleteFiles = 0;
        for (ManifestFile manifest : deleteManifests) {
            PartitionSpec spec = table.specs().get(manifest.partitionSpecId());
            if (spec != null) {
                ManifestEvaluator evaluator = evaluators.computeIfAbsent(
                        manifest.partitionSpecId(),
                        _ -> ManifestEvaluator.forPartitionFilter(
                                Projections.inclusive(spec, caseSensitive).project(dataFilter),
                                spec,
                                caseSensitive));
                if (!evaluator.eval(manifest)) {
                    continue;
                }
            }
            Integer addedFiles = manifest.addedFilesCount();
            Integer existingFiles = manifest.existingFilesCount();
            if (addedFiles == null || existingFiles == null) {
                return summaryDeleteFileCount(snapshot);
            }
            deleteFiles += addedFiles + existingFiles;
        }
        return deleteFiles;
    }

    private static long summaryDeleteFileCount(Snapshot snapshot)
    {
        if (snapshot.summary() == null) {
            return 0;
        }
        String totalDeleteFiles = snapshot.summary().get(TOTAL_DELETE_FILES_PROP);
        if (totalDeleteFiles == null) {
            return 0;
        }
        Long count = Longs.tryParse(totalDeleteFiles);
        if (count == null) {
            return 0;
        }
        return count;
    }
}
