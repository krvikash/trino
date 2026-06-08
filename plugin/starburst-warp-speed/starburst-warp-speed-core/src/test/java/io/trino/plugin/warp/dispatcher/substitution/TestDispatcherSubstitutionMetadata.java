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
package io.trino.plugin.warp.dispatcher.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.SimplifiedColumns;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.connector.substitution.ConnectorSubstitutionMetadata;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDispatcherSubstitutionMetadata
{
    private static final ConnectorSession SESSION = null;
    private static final ConnectorTableHandle PROXY_TABLE_HANDLE = new ConnectorTableHandle() {};
    private static final ColumnHandle COLUMN_HANDLE = new ColumnHandle() {};
    private static final ConnectorTableId TABLE_ID = new ConnectorTableId()
    {
        @Override
        public long hash()
        {
            return 1L;
        }

        @Override
        public ConnectorIdVersion version()
        {
            return new ConnectorIdVersion("table id", 7);
        }
    };
    private static final ConnectorColumnId COLUMN_ID = () -> new ConnectorIdVersion("column id", 11);
    private static final ConnectorStorageTableId STORAGE_TABLE_ID = new ConnectorStorageTableId("schema", "table", "uniqueId");

    private static DispatcherTableHandle dispatcherTableHandle()
    {
        return new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.empty(),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                PROXY_TABLE_HANDLE,
                Optional.empty(),
                List.of(),
                false,
                Set.of());
    }

    @Test
    public void testGetTableIdUnwrapsProxyHandle()
    {
        RecordingSubstitutionMetadata delegate = new RecordingSubstitutionMetadata();
        DispatcherSubstitutionMetadata metadata = new DispatcherSubstitutionMetadata(delegate);

        Optional<ConnectorTableId> tableId = metadata.getTableId(SESSION, dispatcherTableHandle());

        assertThat(delegate.lastTableHandle).isSameAs(PROXY_TABLE_HANDLE);
        assertThat(tableId).contains(TABLE_ID);
    }

    @Test
    public void testGetStorageTableIdUnwrapsProxyHandle()
    {
        RecordingSubstitutionMetadata delegate = new RecordingSubstitutionMetadata();
        DispatcherSubstitutionMetadata metadata = new DispatcherSubstitutionMetadata(delegate);

        Optional<ConnectorStorageTableId> storageTableId = metadata.getStorageTableId(SESSION, dispatcherTableHandle());

        assertThat(delegate.lastTableHandle).isSameAs(PROXY_TABLE_HANDLE);
        assertThat(storageTableId).contains(STORAGE_TABLE_ID);
    }

    @Test
    public void testTableHandleMatchesIdUnwrapsProxyHandle()
    {
        RecordingSubstitutionMetadata delegate = new RecordingSubstitutionMetadata();
        DispatcherSubstitutionMetadata metadata = new DispatcherSubstitutionMetadata(delegate);

        boolean canSubstitute = metadata.tableHandleMatchesId(SESSION, dispatcherTableHandle(), TABLE_ID);

        assertThat(delegate.lastTableHandle).isSameAs(PROXY_TABLE_HANDLE);
        assertThat(delegate.lastCandidateTable).isSameAs(TABLE_ID);
        assertThat(canSubstitute).isTrue();
    }

    @Test
    public void testGetColumnIdForwardsColumnHandle()
    {
        RecordingSubstitutionMetadata delegate = new RecordingSubstitutionMetadata();
        DispatcherSubstitutionMetadata metadata = new DispatcherSubstitutionMetadata(delegate);

        Optional<ConnectorColumnId> columnId = metadata.getColumnId(SESSION, COLUMN_HANDLE);

        assertThat(delegate.lastColumnHandle).isSameAs(COLUMN_HANDLE);
        assertThat(columnId).contains(COLUMN_ID);
    }

    @Test
    public void testVersionsDelegate()
    {
        DispatcherSubstitutionMetadata metadata = new DispatcherSubstitutionMetadata(new RecordingSubstitutionMetadata());

        assertThat(metadata.tableIdVersions()).containsExactly(new ConnectorIdVersion("table id", 7));
        assertThat(metadata.columnIdVersions()).containsExactly(new ConnectorIdVersion("column id", 11));
    }

    private static final class RecordingSubstitutionMetadata
            implements ConnectorSubstitutionMetadata
    {
        private ConnectorTableHandle lastTableHandle;
        private ColumnHandle lastColumnHandle;
        private ConnectorTableId lastCandidateTable;

        @Override
        public boolean tableHandleMatchesId(ConnectorSession session, ConnectorTableHandle queryTable, ConnectorTableId candidateTable)
        {
            lastTableHandle = queryTable;
            lastCandidateTable = candidateTable;
            return true;
        }

        @Override
        public Optional<ConnectorStorageTableId> getStorageTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            lastTableHandle = handle;
            return Optional.of(STORAGE_TABLE_ID);
        }

        @Override
        public Optional<ConnectorTableId> getTableId(ConnectorSession session, ConnectorTableHandle handle)
        {
            lastTableHandle = handle;
            return Optional.of(TABLE_ID);
        }

        @Override
        public Optional<ConnectorColumnId> getColumnId(ConnectorSession session, ColumnHandle column)
        {
            lastColumnHandle = column;
            return Optional.of(COLUMN_ID);
        }

        @Override
        public Set<ConnectorIdVersion> tableIdVersions()
        {
            return ImmutableSet.of(TABLE_ID.version());
        }

        @Override
        public Set<ConnectorIdVersion> columnIdVersions()
        {
            return ImmutableSet.of(COLUMN_ID.version());
        }
    }
}
