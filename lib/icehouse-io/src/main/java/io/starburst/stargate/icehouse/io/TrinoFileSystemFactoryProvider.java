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

import io.starburst.crypto.SecretTransform;
import io.starburst.stargate.icehouse.spi.storage.StorageSpec;
import io.starburst.stargate.icehouse.task.primitives.UncheckedAutoCloseable;
import io.trino.filesystem.TrinoFileSystemFactory;

import static java.util.Objects.requireNonNull;

public interface TrinoFileSystemFactoryProvider
{
    AutoCloseableTrinoFileSystemFactory create(String tracerScopeName, SecretTransform unsealer, StorageSpec storageSpec);

    /**
     * A TrinoFileSystemFactory with an Autocloseable that should be closed when the TrinoFileSystemFactory is cleaned up.
     * TrinoFileSystemFactory does not implement AutoCloseable but some instances of TrinoFileSystemFactory need to be closed
     */
    record AutoCloseableTrinoFileSystemFactory(TrinoFileSystemFactory trinoFileSystemFactory, UncheckedAutoCloseable autoCloseable)
            implements UncheckedAutoCloseable
    {
        public AutoCloseableTrinoFileSystemFactory
        {
            requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
            requireNonNull(autoCloseable, "autoCloseable is null");
        }

        @Override
        public void close()
        {
            autoCloseable.close();
        }
    }
}
