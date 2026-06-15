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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.DeltaLakeTableCredentialsProvider;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUnityVendedCredentialsFileSystemFactory
{
    private static final Location DUMMY_LOCATION = Location.of("s3://bucket/key");
    private static final VendedCredentialsHandle HANDLE = VendedCredentialsHandle.empty("s3://bucket/table");

    @Test
    void testValidCredentialsAreUsedDirectlyWithoutCallingProvider()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        TrackingFileSystemFactory trackingFactory = new TrackingFileSystemFactory();
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new AlwaysValidCredentials("refreshed"))));

        var factory = new UnityVendedCredentialsFileSystemFactory(trackingFactory, provider);
        ConnectorSession session = TestingConnectorSession.SESSION;

        triggerFileSystem(factory.create(session, Optional.of(credentialsWith(new AlwaysValidCredentials("original")))));

        assertThat(providerCallCount.get()).isEqualTo(0);
        assertThat(trackingFactory.lastIdentity().getExtraCredentials()).containsEntry("key", "original");
    }

    @Test
    void testExpiredCredentialsAreRefreshedFromProvider()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        TrackingFileSystemFactory trackingFactory = new TrackingFileSystemFactory();
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new AlwaysValidCredentials("refreshed"))));

        var factory = new UnityVendedCredentialsFileSystemFactory(trackingFactory, provider);
        ConnectorSession session = TestingConnectorSession.SESSION;

        triggerFileSystem(factory.create(session, Optional.of(credentialsWith(new AlwaysExpiredCredentials()))));

        assertThat(providerCallCount.get()).isEqualTo(1);
        assertThat(trackingFactory.lastIdentity().getExtraCredentials()).containsEntry("key", "refreshed");
    }

    @Test
    void testRefreshedCredentialsCachedForSameQueryId()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new AlwaysValidCredentials("refreshed"))));

        var factory = new UnityVendedCredentialsFileSystemFactory(new TrackingFileSystemFactory(), provider);
        ConnectorSession session = TestingConnectorSession.SESSION;
        DeltaLakeTableCredentials credentials = credentialsWith(new AlwaysExpiredCredentials());

        triggerFileSystem(factory.create(session, Optional.of(credentials)));
        triggerFileSystem(factory.create(session, Optional.of(credentials)));

        assertThat(providerCallCount.get()).isEqualTo(1);
    }

    @Test
    void testRefreshedCredentialsNotCachedAcrossDifferentQueryIds()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new AlwaysValidCredentials("refreshed"))));

        var factory = new UnityVendedCredentialsFileSystemFactory(new TrackingFileSystemFactory(), provider);
        DeltaLakeTableCredentials credentials = credentialsWith(new AlwaysExpiredCredentials());

        triggerFileSystem(factory.create(TestingConnectorSession.builder().build(), Optional.of(credentials)));
        assertThat(providerCallCount.get()).isEqualTo(1);
        triggerFileSystem(factory.create(TestingConnectorSession.builder().build(), Optional.of(credentials)));
        assertThat(providerCallCount.get()).isEqualTo(2);
    }

    @Test
    void testExpiredCachedCredentialsAreInvalidatedAndRefetched()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        AtomicBoolean refreshedCredentialsValid = new AtomicBoolean(true);
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new ControllableCredentials(refreshedCredentialsValid))));

        var factory = new UnityVendedCredentialsFileSystemFactory(new TrackingFileSystemFactory(), provider);
        ConnectorSession session = TestingConnectorSession.SESSION;
        DeltaLakeTableCredentials credentials = credentialsWith(new AlwaysExpiredCredentials());

        triggerFileSystem(factory.create(session, Optional.of(credentials)));
        assertThat(providerCallCount.get()).isEqualTo(1);

        refreshedCredentialsValid.set(false);

        triggerFileSystem(factory.create(session, Optional.of(credentials)));
        assertThat(providerCallCount.get()).isEqualTo(2);
    }

    @Test
    void testRefreshedCredentialsCachedForSameQueryIdAndLocation()
    {
        AtomicInteger providerCallCount = new AtomicInteger();
        var provider = new CountingCredentialsProvider(
                providerCallCount,
                () -> Optional.of(credentialsWith(new AlwaysValidCredentials("key"))));

        var factory = new UnityVendedCredentialsFileSystemFactory(new TrackingFileSystemFactory(), provider);
        ConnectorSession session = TestingConnectorSession.SESSION;

        triggerFileSystem(factory.create(session, "s3://bucket/table"));
        assertThat(providerCallCount.get()).isEqualTo(1);
        triggerFileSystem(factory.create(session, "s3://bucket/table"));
        assertThat(providerCallCount.get()).isEqualTo(1);
    }

    private static void triggerFileSystem(TrinoFileSystem fileSystem)
    {
        fileSystem.newInputFile(DUMMY_LOCATION);
    }

    private static DeltaLakeTableCredentials credentialsWith(FileSystemCredentials credentials)
    {
        return new DeltaLakeTableCredentials(HANDLE, credentials);
    }

    private static final class TrackingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private ConnectorIdentity lastIdentity;

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            this.lastIdentity = identity;
            return new NoOpTrinoFileSystem();
        }

        ConnectorIdentity lastIdentity()
        {
            return lastIdentity;
        }
    }

    private record CountingCredentialsProvider(
            AtomicInteger callCount,
            Supplier<Optional<DeltaLakeTableCredentials>> supplier)
            implements DeltaLakeTableCredentialsProvider
    {
        @Override
        public Optional<DeltaLakeTableCredentials> getTableCredentials(VendedCredentialsHandle credentialsHandle)
        {
            callCount.incrementAndGet();
            return supplier.get();
        }
    }

    private record AlwaysValidCredentials(String key)
            implements FileSystemCredentials
    {
        @Override
        public Map<String, String> asExtraCredentials()
        {
            return ImmutableMap.of("key", key);
        }

        @Override
        public boolean isValid()
        {
            return true;
        }
    }

    private static final class AlwaysExpiredCredentials
            implements FileSystemCredentials
    {
        @Override
        public Map<String, String> asExtraCredentials()
        {
            return ImmutableMap.of();
        }

        @Override
        public boolean isValid()
        {
            return false;
        }
    }

    private record ControllableCredentials(AtomicBoolean valid)
            implements FileSystemCredentials
    {
        @Override
        public Map<String, String> asExtraCredentials()
        {
            return ImmutableMap.of();
        }

        @Override
        public boolean isValid()
        {
            return valid.get();
        }
    }

    private static final class NoOpTrinoFileSystem
            implements TrinoFileSystem
    {
        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return null;
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator listFiles(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException();
        }
    }
}
