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

import com.google.inject.Inject;
import io.starburst.crypto.SecretTransform;
import io.starburst.stargate.icehouse.io.TrinoFileSystemFactoryProvider.AutoCloseableTrinoFileSystemFactory;
import io.starburst.stargate.icehouse.spi.storage.S3StorageSpec;
import io.starburst.stargate.icehouse.spi.storage.StorageSpec;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.io.FileIO;

import static java.util.Objects.requireNonNull;

public class FileIoFactory
{
    private final TrinoFileSystemFactoryProvider trinoFileSystemFactoryProvider;

    @Inject
    public FileIoFactory(TrinoFileSystemFactoryProvider trinoFileSystemFactoryProvider)
    {
        this.trinoFileSystemFactoryProvider = requireNonNull(trinoFileSystemFactoryProvider, "trinoFileSystemFactoryProvider is null");
    }

    public FileIO create(SecretTransform unsealer, StorageSpec storageSpec)
    {
        if (storageSpec instanceof S3StorageSpec s3StorageSpec) {
            AutoCloseableTrinoFileSystemFactory autoCloseableFactory = trinoFileSystemFactoryProvider.create("file-io-factory", unsealer, s3StorageSpec);
            return new ForwardingClosingFileIo(
                    new S3ErrorMappingTrinoFileSystem(
                            autoCloseableFactory.trinoFileSystemFactory()
                                    .create(ConnectorIdentity.ofUser("galaxy"))),
                    autoCloseableFactory);
        }
        throw new UnsupportedOperationException("Unsupported storage spec: %s".formatted(storageSpec.getClass()));
    }
}
