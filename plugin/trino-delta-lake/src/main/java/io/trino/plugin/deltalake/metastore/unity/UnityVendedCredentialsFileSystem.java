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

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class UnityVendedCredentialsFileSystem
        implements TrinoFileSystem
{
    private final Supplier<TrinoFileSystem> loader;

    public UnityVendedCredentialsFileSystem(Supplier<TrinoFileSystem> loader)
    {
        this.loader = requireNonNull(loader, "loader is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return fileSystem().newInputFile(location);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return fileSystem().newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return fileSystem().newInputFile(location, length);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return fileSystem().newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return fileSystem().newInputFile(location, length, lastModified);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return fileSystem().newEncryptedInputFile(location, length, lastModified, key);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return fileSystem().newOutputFile(location);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return fileSystem().newEncryptedOutputFile(location, key);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        fileSystem().deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        fileSystem().deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        fileSystem().deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        fileSystem().renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return fileSystem().listFiles(location);
    }

    @Override
    public FileIterator listFilesStartingFrom(Location location, String startingFrom)
            throws IOException
    {
        return fileSystem().listFilesStartingFrom(location, startingFrom);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return fileSystem().directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        fileSystem().createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        fileSystem().renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return fileSystem().listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return fileSystem().createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return fileSystem().preSignedUri(location, ttl);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return fileSystem().encryptedPreSignedUri(location, ttl, key);
    }

    @Override
    public Optional<UriLocation> preSignedPutUri(Location location, Duration ttl, Optional<EncryptionKey> key)
            throws IOException
    {
        return fileSystem().preSignedPutUri(location, ttl, key);
    }

    @Override
    public Optional<UriLocation> preSignedDeleteUri(Location location, Duration ttl)
            throws IOException
    {
        return fileSystem().preSignedDeleteUri(location, ttl);
    }

    private TrinoFileSystem fileSystem()
    {
        return loader.get();
    }
}
