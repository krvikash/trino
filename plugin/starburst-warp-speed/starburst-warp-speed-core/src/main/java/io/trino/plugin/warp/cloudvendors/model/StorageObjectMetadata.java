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
package io.trino.plugin.warp.cloudvendors.model;

import io.trino.plugin.warp.cloudstorage.CloudObjectMetadata;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public record StorageObjectMetadata(
        Map<String, String> userMetadata,
        Map<String, Object> metadata)
{
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String LAST_MODIFIED = "Last-Modified";
    public static final String ETAG = "eTag";
    public static final String ETAG_UNKNOWN = "eTagUnknown";

    public StorageObjectMetadata()
    {
        this(new HashMap<>(), new HashMap<>());
    }

    public StorageObjectMetadata(CloudObjectMetadata metadata)
    {
        this(new HashMap<>(), new HashMap<>());
        metadata.getLastModified().ifPresent(lastModified -> setLastModified(lastModified.toEpochMilli()));
        metadata.getContentLength().ifPresent(this::setContentLength);
        metadata.getETag().ifPresent(this::setETag);
    }

    public Object getMetadata(String key)
    {
        return metadata.get(key);
    }

    public void putMetadata(String key, Object value)
    {
        metadata.put(key, value);
    }

    public Optional<Long> getContentLength()
    {
        return Optional.ofNullable((Long) getMetadata(CONTENT_LENGTH));
    }

    public void setContentLength(long contentLength)
    {
        putMetadata(CONTENT_LENGTH, contentLength);
    }

    public Optional<Long> getLastModified()
    {
        return Optional.ofNullable((Long) getMetadata(LAST_MODIFIED));
    }

    public void setLastModified(long lastModified)
    {
        putMetadata(LAST_MODIFIED, lastModified);
    }

    public Optional<String> getETag()
    {
        return Optional.ofNullable((String) getMetadata(ETAG));
    }

    public void setETag(String eTag)
    {
        putMetadata(ETAG, eTag);
    }

    public CloudObjectMetadata getCloudObjectMetadata()
    {
        String eTag = (String) getMetadata(ETAG);
        Long lastModified = (Long) getMetadata(LAST_MODIFIED);
        return new CloudObjectMetadata(
                ((eTag != null) && !eTag.equals(ETAG_UNKNOWN)) ? eTag : null,
                (lastModified != null) ? Instant.ofEpochMilli(lastModified) : null,
                (Long) getMetadata(CONTENT_LENGTH));
    }
}
