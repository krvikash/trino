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
package io.starburst.stargate.icehouse.catalog;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * {@link TrinoFileSystemFactory} stub for listing-only paths. Backends require a
 * non-null factory at construction but discovery never touches a file; any
 * filesystem call throws.
 */
public final class ListingOnlyTrinoFileSystemFactory
        implements TrinoFileSystemFactory
{
    public static final ListingOnlyTrinoFileSystemFactory INSTANCE = new ListingOnlyTrinoFileSystemFactory();

    private static final String UNSUPPORTED_MESSAGE = "Filesystem operations are not supported for catalog listing";

    private ListingOnlyTrinoFileSystemFactory() {}

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new UnsupportedTrinoFileSystem();
    }

    private static class UnsupportedTrinoFileSystem
            implements TrinoFileSystem
    {
        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
        }
    }
}
