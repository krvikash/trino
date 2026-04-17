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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.expressions.Expression;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class MergeOnRead
        implements UpdateSnapshot
{
    private final RowDelta rowDelta;

    public MergeOnRead(RowDelta rowDelta)
    {
        this.rowDelta = requireNonNull(rowDelta, "rowDelta is null");
    }

    @Override
    public void validateFromSnapshot(long snapshotId)
    {
        rowDelta.validateFromSnapshot(snapshotId);
    }

    @Override
    public void conflictDetectionFilter(Expression conflictDetectionFilter)
    {
        rowDelta.conflictDetectionFilter(conflictDetectionFilter);
    }

    @Override
    public void validateNoConflicting()
    {
        rowDelta.validateNoConflictingDataFiles();
    }

    @Override
    public void validateNoConflictingDeleteFiles()
    {
        rowDelta.validateDeletedFiles();
        rowDelta.validateNoConflictingDeleteFiles();
    }

    @Override
    public void validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles)
    {
        rowDelta.validateDataFilesExist(referencedFiles);
    }

    @Override
    public void addDeletes(DeleteFile deletes)
    {
        rowDelta.addDeletes(deletes);
    }

    @Override
    public void addRows(Optional<DataFile> dataFile, Optional<DataFile> rewrittenDataFile)
    {
        rowDelta.addRows(dataFile.orElseThrow());
    }

    @Override
    public void scanManifestsWith(ExecutorService executorService)
    {
        rowDelta.scanManifestsWith(executorService);
    }

    @Override
    public void toBranch(String branch)
    {
        rowDelta.toBranch(branch);
    }

    @Override
    public SnapshotUpdate<?> unwrap()
    {
        return rowDelta;
    }
}
