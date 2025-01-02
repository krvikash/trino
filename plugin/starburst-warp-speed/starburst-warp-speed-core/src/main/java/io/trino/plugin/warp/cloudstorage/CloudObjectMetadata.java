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
package io.trino.plugin.warp.cloudstorage;

import java.time.Instant;
import java.util.Optional;

public class CloudObjectMetadata
{
    private final Optional<String> eTag;
    private final Optional<Instant> lastModified;
    private final Optional<Long> contentLength;

    public CloudObjectMetadata(String eTag, Instant lastModified, Long contentLength)
    {
        this.eTag = Optional.ofNullable(eTag);
        this.lastModified = Optional.ofNullable(lastModified);
        this.contentLength = Optional.ofNullable(contentLength);
    }

    public CloudObjectMetadata()
    {
        this(null, null, null);
    }

    public Optional<String> getETag()
    {
        return eTag;
    }

    public Optional<Instant> getLastModified()
    {
        return lastModified;
    }

    public Optional<Long> getContentLength()
    {
        return contentLength;
    }
}
