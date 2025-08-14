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
package io.trino.filesystem;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.Decorator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DecoratingTrinoFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final Set<Decorator<TrinoFileSystem>> decorators;

    public DecoratingTrinoFileSystemFactory(TrinoFileSystemFactory delegate, Set<Decorator<TrinoFileSystem>> decorators)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.decorators = ImmutableSet.copyOf(requireNonNull(decorators, "decorators is null"));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return Decorator.combine(() -> delegate.create(identity), decorators);
    }

    @Override
    public TrinoFileSystem create(ConnectorSession session)
    {
        return Decorator.combine(() -> delegate.create(session), decorators);
    }
}
