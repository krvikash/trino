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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatcherSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final String path;
    private final long start;
    private final long length;
    private final long fileModifiedTime;

    private final List<PartitionKey> partitionKeys;
    private final String deletedFilesHash;
    private final ConnectorSplit proxyConnectorSplit;

    @JsonCreator
    public DispatcherSplit(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("partitionKeys") List<PartitionKey> partitionKeys,
            @JsonProperty("deletedFilesHash") String deletedFilesHash,
            @JsonProperty("proxyConnectorSplit") ConnectorSplit proxyConnectorSplit)
    {
        this.schemaName = requireNonNull(schemaName);
        this.tableName = requireNonNull(tableName);
        this.path = requireNonNull(path);
        this.start = start;
        this.length = length;
        this.fileModifiedTime = fileModifiedTime;
        this.partitionKeys = requireNonNull(partitionKeys);
        this.deletedFilesHash = requireNonNull(deletedFilesHash);
        this.proxyConnectorSplit = requireNonNull(proxyConnectorSplit);
    }

    @JsonProperty("schemaName")
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty("tableName")
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty("path")
    public String getPath()
    {
        return path;
    }

    @JsonProperty("start")
    public long getStart()
    {
        return start;
    }

    @JsonProperty("length")
    public long getLength()
    {
        return length;
    }

    @JsonProperty("fileModifiedTime")
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty("partitionKeys")
    public List<PartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty("deletedFilesHash")
    public String getDeletedFilesHash()
    {
        return deletedFilesHash;
    }

    @JsonProperty("proxyConnectorSplit")
    public ConnectorSplit getProxyConnectorSplit()
    {
        return proxyConnectorSplit;
    }

    // routes splits for the same file range to the same worker; not needed on workers
    @Override
    @JsonIgnore
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DispatcherSplit that = (DispatcherSplit) o;
        return start == that.start && length == that.length &&
                fileModifiedTime == that.fileModifiedTime &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(path, that.path) &&
                Objects.equals(partitionKeys, that.partitionKeys) &&
                Objects.equals(deletedFilesHash, that.deletedFilesHash) &&
                Objects.equals(proxyConnectorSplit, that.proxyConnectorSplit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, path, start, length, fileModifiedTime, partitionKeys, deletedFilesHash, proxyConnectorSplit);
    }

    @Override
    public String toString()
    {
        return "DispatcherSplit{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", path='" + path + '\'' +
                ", start=" + start +
                ", length=" + length +
                ", fileModifiedTime=" + fileModifiedTime +
                ", partitionKeys=" + partitionKeys +
                ", deletedFilesHash='" + deletedFilesHash + '\'' +
                ", proxyConnectorSplit=" + proxyConnectorSplit +
                '}';
    }
}
