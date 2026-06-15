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
package io.starburst.stargate.icehouse.io;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.icehouse.io.storage.exceptions.AccessDeniedStorageException;
import io.starburst.stargate.icehouse.io.storage.exceptions.InterruptedStorageException;
import io.starburst.stargate.icehouse.io.storage.exceptions.NotFoundStorageException;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class S3ErrorMappingTrinoFileSystem
        implements TrinoFileSystem
{
    private static final Set<String> S3_NOT_FOUND_ERROR_CODES = ImmutableSet.<String>builder()
            .add("NoSuchBucket")
            .add("NoSuchKey")
            .build();
    private static final Set<String> S3_ACCESS_DENIED_ERROR_CODES = ImmutableSet.<String>builder()
            .add("AccessDenied")
            .add("AllAccessDisabled")
            .add("InvalidAccessKeyId")
            .add("SignatureDoesNotMatch")
            .build();
    private static final Set<String> STS_ACCESS_DENIED_ERROR_CODES = ImmutableSet.<String>builder()
            .add("AccessDenied")
            .build();
    private final TrinoFileSystem delegate;

    public S3ErrorMappingTrinoFileSystem(TrinoFileSystem delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    private static void tryRethrowException(SdkException exception)
    {
        switch (exception) {
            case S3Exception e when S3_NOT_FOUND_ERROR_CODES.contains(e.awsErrorDetails().errorCode()) -> throw new NotFoundStorageException(e.getMessage(), e);
            case S3Exception e when S3_ACCESS_DENIED_ERROR_CODES.contains(e.awsErrorDetails().errorCode()) -> throw new AccessDeniedStorageException(e.getMessage(), e);
            case StsException e when STS_ACCESS_DENIED_ERROR_CODES.contains(e.awsErrorDetails().errorCode()) -> throw new AccessDeniedStorageException(e.getMessage(), e);
            case AbortedException e -> throw new InterruptedStorageException(e);
            default -> {}
        }
    }

    private static void tryRethrowException(Exception exception)
    {
        switch (exception) {
            case SdkException e -> tryRethrowException(e);
            case IOException e when e.getCause() != null && (e.getCause() instanceof SdkException sdkException) -> tryRethrowException(sdkException);
            default -> {}
        }
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        try {
            return new S3ErrorMappingTrinoInputFile(delegate.newInputFile(location));
        }
        catch (SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        try {
            return new S3ErrorMappingTrinoInputFile(delegate.newInputFile(location, length));
        }
        catch (SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        try {
            return new S3ErrorMappingTrinoInputFile(delegate.newInputFile(location, length, lastModified));
        }
        catch (SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        try {
            return new S3ErrorMappingTrinoOutputFile(delegate.newOutputFile(location));
        }
        catch (SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        try {
            delegate.deleteFile(location);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        try {
            delegate.deleteDirectory(location);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        try {
            delegate.renameFile(source, target);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        try {
            return new S3ErrorMappingFileIterator(delegate.listFiles(location));
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        try {
            return delegate.directoryExists(location);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        try {
            delegate.createDirectory(location);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        try {
            delegate.renameDirectory(source, target);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        try {
            return delegate.listDirectories(location);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        try {
            return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
        }
        catch (IOException | SdkException e) {
            tryRethrowException(e);
            throw e;
        }
    }

    public static class S3ErrorMappingInput
            implements TrinoInput
    {
        private final TrinoInput delegate;

        public S3ErrorMappingInput(TrinoInput delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            try {
                delegate.readFully(position, buffer, bufferOffset, bufferLength);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            try {
                return delegate.readTail(buffer, bufferOffset, bufferLength);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                delegate.close();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }

    public static class S3ErrorMappingInputStream
            extends TrinoInputStream
    {
        private final TrinoInputStream delegate;

        public S3ErrorMappingInputStream(TrinoInputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int available()
                throws IOException
        {
            try {
                return delegate.available();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public long getPosition()
                throws IOException
        {
            try {
                return delegate.getPosition();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void seek(long position)
                throws IOException
        {
            try {
                delegate.seek(position);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public int read()
                throws IOException
        {
            try {
                return delegate.read();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                throws IOException
        {
            try {
                return delegate.read(bytes, offset, length);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public long skip(long n)
                throws IOException
        {
            try {
                return delegate.skip(n);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void skipNBytes(long n)
                throws IOException
        {
            try {
                delegate.skipNBytes(n);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                delegate.close();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }

    public static class S3ErrorMappingTrinoInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;

        public S3ErrorMappingTrinoInputFile(TrinoInputFile delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            try {
                return new S3ErrorMappingInput(delegate.newInput());
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            try {
                return new S3ErrorMappingInputStream(delegate.newStream());
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public long length()
                throws IOException
        {
            try {
                return delegate.length();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            try {
                return delegate.lastModified();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public boolean exists()
                throws IOException
        {
            try {
                return delegate.exists();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public Location location()
        {
            try {
                return delegate.location();
            }
            catch (SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }

    public static class S3ErrorMappingTrinoOutputFile
            implements TrinoOutputFile
    {
        private final TrinoOutputFile delegate;

        public S3ErrorMappingTrinoOutputFile(TrinoOutputFile delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void createOrOverwrite(byte[] data)
                throws IOException
        {
            try {
                delegate.createOrOverwrite(data);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            try {
                return new S3ErrorMappingOutputStream(delegate.create(memoryContext));
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public Location location()
        {
            try {
                return delegate.location();
            }
            catch (SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }

    public static class S3ErrorMappingOutputStream
            extends OutputStream
    {
        private final OutputStream delegate;

        public S3ErrorMappingOutputStream(OutputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void write(int b)
                throws IOException
        {
            try {
                delegate.write(b);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            try {
                delegate.write(bytes, offset, length);
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void flush()
                throws IOException
        {
            try {
                delegate.flush();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                delegate.close();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }

    public static class S3ErrorMappingFileIterator
            implements FileIterator
    {
        private final FileIterator delegate;

        public S3ErrorMappingFileIterator(FileIterator delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public boolean hasNext()
                throws IOException
        {
            try {
                return delegate.hasNext();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }

        @Override
        public FileEntry next()
                throws IOException
        {
            try {
                return delegate.next();
            }
            catch (IOException | SdkException e) {
                tryRethrowException(e);
                throw e;
            }
        }
    }
}
