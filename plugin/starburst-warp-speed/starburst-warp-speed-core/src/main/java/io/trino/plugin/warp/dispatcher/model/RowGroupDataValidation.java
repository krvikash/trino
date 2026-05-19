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
package io.trino.plugin.warp.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.cloudvendors.model.StorageObjectMetadata;

import java.io.Serializable;
import java.util.Objects;

public record RowGroupDataValidation(
        @JsonProperty("file_etag") String fileETag,
        @JsonProperty("file_modified_time") long fileModifiedTime,
        @JsonProperty("file_content_length") long fileContentLength)
        implements Serializable
{
    public static final RowGroupDataValidation EMPTY_VALIDATION = new RowGroupDataValidation(null, 0, 0);

    @JsonCreator
    public RowGroupDataValidation {}

    public RowGroupDataValidation(StorageObjectMetadata storageObjectMetadata)
    {
        this(storageObjectMetadata.getETag().orElse(null),
                storageObjectMetadata.getLastModified().orElse(0L),
                storageObjectMetadata.getContentLength().orElse(0L));
    }

    public StorageObjectMetadata getStorageObjectMetadata()
    {
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        if (fileETag != null) {
            storageObjectMetadata.setETag(fileETag);
        }
        if (fileModifiedTime != 0) {
            storageObjectMetadata.setLastModified(fileModifiedTime);
        }
        if (fileContentLength != 0) {
            storageObjectMetadata.setContentLength(fileContentLength);
        }
        return storageObjectMetadata;
    }

    public boolean isValid()
    {
        return (fileETag != null) || (fileModifiedTime != 0) || (fileContentLength != 0);
    }

    @Override
    public boolean equals(Object object)
    {
        if ((object == null) || (getClass() != object.getClass())) {
            return false;
        }
        RowGroupDataValidation that = (RowGroupDataValidation) object;
        return ((fileModifiedTime == 0) || (that.fileModifiedTime == 0) || (fileModifiedTime == that.fileModifiedTime)) &&
                (fileContentLength == that.fileContentLength) &&
                ((fileETag == null) || fileETag.equals(StorageObjectMetadata.ETAG_UNKNOWN) ||
                        (that.fileETag == null) || that.fileETag.equals(StorageObjectMetadata.ETAG_UNKNOWN) ||
                        Objects.equals(fileETag, that.fileETag));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileETag, fileModifiedTime, fileContentLength);
    }
}
