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

import io.starburst.stargate.icehouse.task.primitives.UncheckedAutoCloseable;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * TrinoFileSystem does not implement AutoCloseable, so we need to pass in a separate object that can actually clean up the resources.
 */
public class ForwardingClosingFileIo
        extends ForwardingFileIo
{
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final UncheckedAutoCloseable autoCloseable;

    public ForwardingClosingFileIo(TrinoFileSystem fileSystem, UncheckedAutoCloseable autoCloseable)
    {
        super(fileSystem, true);
        this.autoCloseable = requireNonNull(autoCloseable, "autoCloseable is null");
    }

    @Override
    public void close()
    {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        autoCloseable.close();
    }
}
