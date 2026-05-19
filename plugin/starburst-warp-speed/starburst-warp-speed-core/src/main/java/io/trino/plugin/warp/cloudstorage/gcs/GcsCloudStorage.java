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
package io.trino.plugin.warp.cloudstorage.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsLocation;
import io.trino.plugin.warp.cloudstorage.CloudObjectMetadata;
import io.trino.plugin.warp.cloudstorage.CloudStorageService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class GcsCloudStorage
        extends CloudStorageService
{
    private final Storage storage;

    public GcsCloudStorage(GcsFileSystemFactory fileSystemFactory, Storage storage)
    {
        super(fileSystemFactory);
        this.storage = requireNonNull(storage, "storage is null");
    }

    @Override
    public CloudObjectMetadata uploadFile(Location source, Location target)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(target);
        checkIsValidFile(gcsLocation);

        BlobInfo blobInfo = BlobInfo.newBuilder(GcsUtils.getBlobId(gcsLocation)).build();
        Optional<Blob> blob = GcsUtils.getBlob(storage, gcsLocation);

        Storage.BlobWriteOption precondition = blob.map(value -> Storage.BlobWriteOption.generationMatch(value.getGeneration()))
                .orElseGet(Storage.BlobWriteOption::doesNotExist);

        Blob targetBlob = storage.createFrom(blobInfo, Path.of(source.toString()), precondition);

        return new CloudObjectMetadata(
                targetBlob.getEtag(),
                targetBlob.getTimeStorageClassUpdatedOffsetDateTime().toInstant(),
                targetBlob.getSize());
    }

    @Override
    public CloudObjectMetadata downloadFile(Location source, Location target)
    {
        GcsLocation gcsLocation = new GcsLocation(source);
        checkIsValidFile(gcsLocation);

        GcsUtils.getBlob(storage, gcsLocation).ifPresent(blob -> blob.downloadTo(Path.of(target.toString())));

        // ToDo: metadata
        return new CloudObjectMetadata();
    }

    @Override
    public CloudObjectMetadata copyFile(Location source, Location destination)
    {
        GcsLocation sourceLocation = new GcsLocation(source);
        GcsLocation targetLocation = new GcsLocation(destination);

        checkIsValidFile(sourceLocation);
        checkIsValidFile(targetLocation);

        Optional<Blob> blob = GcsUtils.getBlob(storage, sourceLocation);

        if (blob.isEmpty()) {
            return new CloudObjectMetadata();
        }

        Blob destinationBlob = blob.get().copyTo(GcsUtils.getBlobId(targetLocation)).getResult();

        return new CloudObjectMetadata(
                destinationBlob.getEtag(),
                destinationBlob.getTimeStorageClassUpdatedOffsetDateTime().toInstant(),
                destinationBlob.getSize());
    }

    @Override
    public CloudObjectMetadata renameFile(Location source, Location target)
            throws IOException
    {
        CloudObjectMetadata metadata = copyFile(source, target);
        deleteFile(source);
        return metadata;
    }

    private static void checkIsValidFile(GcsLocation gcsLocation)
    {
        checkState(!gcsLocation.path().isEmpty(), "Location path is empty: %s", gcsLocation);
        checkState(!gcsLocation.path().endsWith("/"), "Location path ends with a slash: %s", gcsLocation);
    }
}
