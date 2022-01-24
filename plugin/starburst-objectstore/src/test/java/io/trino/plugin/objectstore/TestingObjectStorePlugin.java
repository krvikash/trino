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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingObjectStorePlugin
        extends ObjectStorePlugin
{
    private final String connectorName;
    private final Optional<HiveMetastore> metastore;
    private final Optional<Module> hiveModule;
    private final Optional<Path> localFileSystemRootPath;

    public TestingObjectStorePlugin(String connectorName, Optional<HiveMetastore> metastore, Optional<Module> hiveModule, Optional<Path> localFileSystemRootPath)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hiveModule = requireNonNull(hiveModule, "hiveModule is null");
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TestingObjectStoreConnectorFactory(connectorName, metastore, hiveModule, localFileSystemRootPath));
    }
}
