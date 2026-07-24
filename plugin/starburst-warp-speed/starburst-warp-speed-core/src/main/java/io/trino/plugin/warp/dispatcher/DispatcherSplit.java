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
package io.trino.plugin.warp.dispatcher;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DispatcherSplit(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("path") String path,
        @JsonProperty("start") long start,
        @JsonProperty("length") long length,
        @JsonProperty("fileModifiedTime") long fileModifiedTime,
        @JsonProperty("partitionKeys") List<PartitionKey> partitionKeys,
        @JsonProperty("deletedFilesHash") String deletedFilesHash,
        @JsonProperty("proxyConnectorSplit") ConnectorSplit proxyConnectorSplit)
        implements ConnectorSplit
{
    public DispatcherSplit
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(path, "path is null");
        partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        requireNonNull(deletedFilesHash, "deletedFilesHash is null");
        requireNonNull(proxyConnectorSplit, "proxyConnectorSplit is null");
    }

    // routes splits for the same file range to the same worker
    @Override
    public Optional<String> getAffinityKey()
    {
        return Optional.of(path + ":" + start + ":" + length);
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return proxyConnectorSplit.getSplitWeight();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return proxyConnectorSplit.getRetainedSizeInBytes();
    }
}
