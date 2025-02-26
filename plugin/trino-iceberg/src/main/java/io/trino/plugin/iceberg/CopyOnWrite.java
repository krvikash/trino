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
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.expressions.Expression;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CopyOnWrite
        implements UpdateSnapshot
{
    private final OverwriteFiles overwriteFiles;

    public CopyOnWrite(OverwriteFiles overwriteFiles)
    {
        this.overwriteFiles = requireNonNull(overwriteFiles, "overwriteFiles is null");
    }

    @Override
    public void validateFromSnapshot(long snapshotId)
    {
        overwriteFiles.validateFromSnapshot(snapshotId);
    }

    @Override
    public void conflictDetectionFilter(Expression conflictDetectionFilter)
    {
        overwriteFiles.conflictDetectionFilter(conflictDetectionFilter);
    }

    @Override
    public void validateNoConflicting()
    {
        overwriteFiles.validateNoConflictingData();
    }

    @Override
    public void validateNoConflictingDeleteFiles()
    {
        overwriteFiles.validateNoConflictingDeletes();
    }

    @Override
    public void validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles) {}

    @Override
    public void removeDeletes(DeleteFile deletes)
    {
        throw new UnsupportedOperationException("CoW doesn't support removeDeletes");
    }

    @Override
    public void addDeletes(DeleteFile deletes)
    {
        throw new UnsupportedOperationException("CoW doesn't support addDeletes");
    }

    @Override
    public void addRows(Optional<DataFile> dataFile, Optional<DataFile> rewrittenDataFile)
    {
        checkArgument(dataFile.isPresent() || rewrittenDataFile.isPresent(), "At least one of dataFile or rewrittenDataFile must be present");
        // When the data in file all removed, just delete the file, no need to add a new data file
        dataFile.ifPresent(overwriteFiles::addFile);
        rewrittenDataFile.ifPresent(overwriteFiles::deleteFile);
    }

    @Override
    public void scanManifestsWith(ExecutorService executorService)
    {
        overwriteFiles.scanManifestsWith(executorService);
    }

    @Override
    public void toBranch(String branch)
    {
        overwriteFiles.toBranch(branch);
    }

    @Override
    public SnapshotUpdate<?> unwrap()
    {
        return overwriteFiles;
    }
}
