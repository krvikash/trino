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
package io.trino.testing.mock;

import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Set;

import static io.trino.testing.mock.CompiledWithOssCustomType.CUSTOM_TYPE;

public class CompiledWithOssPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of(new CompiledWithOssConnectorFactory());
    }

    @Override
    public Iterable<Type> getTypes()
    {
        return List.of(CUSTOM_TYPE);
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return Set.of(CompiledWithOssFunctions.class);
    }
}
