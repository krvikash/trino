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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.function.Function;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

public class TestDecoratingTrinoFileSystemFactory
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(TrinoFileSystemFactory.class, DecoratingTrinoFileSystemFactory.class);
    }

    @Test
    public void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(
                TrinoFileSystemFactory.class,
                factory -> new DecoratingTrinoFileSystemFactory(
                        factory,
                        ImmutableSet.of(decoratedFactory -> decoratedFactory)));
    }

    @Test
    public void testFileSystemsAreDecorated()
            throws Exception
    {
        TrinoFileSystem undecorated = new ForwardingTrinoFileSystem()
        {
            @Override
            protected TrinoFileSystem delegate()
            {
                throw new IllegalStateException();
            }
        };

        Function<TrinoFileSystem, TrinoFileSystemFactory> forwardingFactory = fileSystem -> new DecoratingTrinoFileSystemFactory(
                new TrinoFileSystemFactory()
                {
                    @Override
                    public TrinoFileSystem create(ConnectorIdentity identity)
                    {
                        return undecorated;
                    }

                    @Override
                    public TrinoFileSystem create(ConnectorSession session)
                    {
                        return undecorated;
                    }
                },
                ImmutableSet.of(_ -> fileSystem));

        assertProperForwardingMethodsAreCalled(
                TrinoFileSystem.class,
                forwardingFactory.andThen(factory -> factory.create((ConnectorIdentity) null)),
                Set.of(
                        TrinoFileSystem.class.getMethod("isUnrecoverableException", Throwable.class),
                        // checkStartingFrom is static and cannot be overridden:
                        TrinoFileSystem.class.getMethod("checkStartingFrom", String.class)));

        assertProperForwardingMethodsAreCalled(
                TrinoFileSystem.class,
                forwardingFactory.andThen(factory -> factory.create((ConnectorSession) null)),
                Set.of(
                        TrinoFileSystem.class.getMethod("isUnrecoverableException", Throwable.class),
                        // checkStartingFrom is static and cannot be overridden:
                        TrinoFileSystem.class.getMethod("checkStartingFrom", String.class)));
    }
}
