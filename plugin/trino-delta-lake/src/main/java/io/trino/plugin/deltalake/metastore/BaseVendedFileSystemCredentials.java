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
package io.trino.plugin.deltalake.metastore;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public abstract class BaseVendedFileSystemCredentials
        implements FileSystemCredentials
{
    private static final int VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS = 120;

    private final Instant expireAt;

    public BaseVendedFileSystemCredentials(Instant expireAt)
    {
        this.expireAt = requireNonNull(expireAt, "expireAt is null");
    }

    @Override
    public boolean isValid()
    {
        // If the token expires after 2 mins, don't use it
        // TODO: make the time configurable
        return Instant.now().isBefore(expireAt().minusSeconds(VALID_BUT_NOT_USABLE_THRESHOLD_SECONDS));
    }

    public Instant expireAt()
    {
        return expireAt;
    }
}
