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
package io.trino.plugin.iceberg.substitution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.substitution.ConnectorStorageTableId;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergSubstitutionMetadata
{
    @Test
    public void testGetStorageTableIdReturnsEmptyWhenStorageHasNoSnapshot()
    {
        // A storage table with no current snapshot (e.g. never refreshed) cannot pin a snapshot,
        // so getStorageTableId must return empty and let the engine skip indexing rather than
        // throwing.
        IcebergSubstitutionMetadata metadata = new IcebergSubstitutionMetadata();
        IcebergTableHandle noSnapshotHandle = newHandle(OptionalLong.empty());

        Optional<ConnectorStorageTableId> result = metadata.getStorageTableId(
                TestingConnectorSession.SESSION,
                noSnapshotHandle);

        assertThat(result).isEmpty();
    }

    @Test
    public void testGetStorageTableIdPinsCurrentSnapshot()
    {
        IcebergSubstitutionMetadata metadata = new IcebergSubstitutionMetadata();
        IcebergTableHandle handle = newHandle(OptionalLong.of(42));

        Optional<ConnectorStorageTableId> result = metadata.getStorageTableId(
                TestingConnectorSession.SESSION,
                handle);

        assertThat(result).contains(new ConnectorStorageTableId("s", "t", "42"));
    }

    private static IcebergTableHandle newHandle(OptionalLong snapshotId)
    {
        return new IcebergTableHandle(
                "s",
                "t",
                TableType.DATA,
                snapshotId,
                "{}",
                OptionalInt.empty(),
                ImmutableMap.of(),
                2,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                false,
                OptionalInt.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "loc",
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                false,
                UUID.randomUUID(),
                false,
                Optional.empty(),
                false,
                ImmutableSet.of(),
                Optional.empty());
    }
}
