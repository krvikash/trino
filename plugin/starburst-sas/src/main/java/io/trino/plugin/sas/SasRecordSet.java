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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SasRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(SasRecordSet.class);

    private final List<SasColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String file;
    private final long start;
    private final long pageCount;
    private final SasClient client;
    private final TrinoFileSystem fileSystem;

    public SasRecordSet(SasSplit split, List<SasColumnHandle> columnHandles, SasClient client, TrinoFileSystem fileSystem)
    {
        requireNonNull(split, "split is null");
        this.pageCount = split.pageCount();
        this.client = requireNonNull(client, "client is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.file = split.uri();
        this.start = split.start();
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.columnTypes = columnHandles.stream()
                .map(SasColumnHandle::columnType)
                .collect(toImmutableList());
        log.debug("Sas record set: %s", split.uri());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new SasRecordCursor(columnHandles, file, start, pageCount, client, fileSystem);
    }
}
