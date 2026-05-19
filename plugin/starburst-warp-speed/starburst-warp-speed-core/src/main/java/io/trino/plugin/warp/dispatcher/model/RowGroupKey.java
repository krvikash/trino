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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.tools.util.PathUtils;
import io.trino.plugin.warp.tools.util.StringUtils;

import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import java.util.StringJoiner;

public final class RowGroupKey
        implements Serializable
{
    // how many slashes to skip in file name to reach the file path and offset/length/file-modification-time part
    public static final int FILE_NAME_START_OF_FILE_NAME = 6;
    @Serial
    private static final long serialVersionUID = 0L;
    @JsonProperty("schema_name")
    private final String schema;
    @JsonProperty("table_name")
    private final String table;
    @JsonProperty("file_path")
    private final String filePath;
    @JsonProperty("file_offset")
    private final long offset;
    @JsonProperty("length")
    private final long length;
    @JsonProperty("file_modified_time")
    private final long fileModifiedTime;
    @JsonProperty("deleted_files_hash")
    private final String deletedFilesHash;
    @JsonProperty("catalog_name")
    private final String catalogName;

    @JsonIgnore
    private int hashCode;

    @JsonCreator
    public RowGroupKey(@JsonProperty("schema_name") String schema, @JsonProperty("table_name") String table, @JsonProperty("file_path") String filePath, @JsonProperty("file_offset") long offset, @JsonProperty("length") long length, @JsonProperty("file_modified_time") long fileModifiedTime, @JsonProperty("deleted_files_hash") String deletedFilesHash, @JsonProperty("catalog_name") String catalogName)
    {
        this.schema = schema;
        this.table = table;
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
        this.fileModifiedTime = fileModifiedTime;
        this.deletedFilesHash = deletedFilesHash;
        this.catalogName = catalogName;
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(":");
        stringJoiner.add(schema)
                .add(table)
                .add(getFilePathWithoutPrefix())
                .add(Long.toString(offset))
                .add(Long.toString(length))
                .add(Long.toString(fileModifiedTime));
        if (!StringUtils.isEmpty(deletedFilesHash)) {
            stringJoiner.add(deletedFilesHash);
        }
        return stringJoiner.toString().replaceAll("\\s+", "_");
    }

    public String stringFileNameRepresentation(String storePath)
    {
        Path path = Path.of(
                schema,
                table,
                getFilePathWithoutPrefix(),
                Long.toString(offset),
                Long.toString(length),
                Long.toString(fileModifiedTime));
        if (!StringUtils.isEmpty(deletedFilesHash)) {
            path = Path.of(path.toString(), deletedFilesHash);
        }
        String fileName = path.toString().replaceAll("\\s+", "_");
        return PathUtils.getUriPath(storePath, catalogName, fileName);
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
        RowGroupKey that = (RowGroupKey) o;
        return offset == that.offset && length == that.length && fileModifiedTime == that.fileModifiedTime && Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(getFilePathWithoutPrefix(), that.getFilePathWithoutPrefix()) && Objects.equals(deletedFilesHash, that.deletedFilesHash) && Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        if (hashCode == 0) {
            hashCode = Objects.hash(schema, table, getFilePathWithoutPrefix(), offset, length, fileModifiedTime, deletedFilesHash, catalogName);
        }
        return hashCode;
    }

    private String getFilePathWithoutPrefix()
    {
        String filePathWithoutPrefix = filePath;
        if (filePathWithoutPrefix.contains("//")) {
            filePathWithoutPrefix = filePathWithoutPrefix.substring(filePathWithoutPrefix.indexOf("//") + "//".length());
        }
        return filePathWithoutPrefix.replaceAll("\\s+", "_"); // in UNIX file system a file name can't contain spaces
    }

    @JsonProperty("schema_name")
    public String schema()
    {
        return schema;
    }

    @JsonProperty("table_name")
    public String table()
    {
        return table;
    }

    @JsonProperty("file_path")
    public String filePath()
    {
        return filePath;
    }

    @JsonProperty("file_offset")
    public long offset()
    {
        return offset;
    }

    @JsonProperty("length")
    public long length()
    {
        return length;
    }

    @JsonProperty("file_modified_time")
    public long fileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty("deleted_files_hash")
    public String deletedFilesHash()
    {
        return deletedFilesHash;
    }

    @JsonProperty("catalog_name")
    public String catalogName()
    {
        return catalogName;
    }
}
