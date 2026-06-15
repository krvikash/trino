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
package io.starburst.stargate.icehouse.catalog.glue;

import com.google.inject.Provider;
import io.airlift.configuration.ConfigurationFactory;
import io.starburst.stargate.icehouse.catalog.CatalogKind;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalogFactory;
import io.starburst.stargate.icehouse.spi.maintenance.PlaintextTrinoProperties;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import jakarta.inject.Inject;
import org.apache.iceberg.io.FileIO;
import software.amazon.awssdk.services.glue.GlueClient;

import static java.util.Objects.requireNonNull;

/**
 * Builds a {@link GlueIcehouseCatalog} from already-translated plaintext properties.
 */
public final class GlueIcehouseCatalogFactory
        implements IcehouseCatalogFactory
{
    private final Provider<GlueClientFactory> glueClientFactoryProvider;
    private final TypeManager typeManager;

    @Inject
    public GlueIcehouseCatalogFactory(
            Provider<GlueClientFactory> glueClientFactoryProvider,
            TypeManager typeManager)
    {
        this.glueClientFactoryProvider = requireNonNull(glueClientFactoryProvider, "glueClientFactoryProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public CatalogKind kind()
    {
        return CatalogKind.GLUE;
    }

    @Override
    public IcehouseCatalog create(AccountId accountId, CatalogId catalogId, PlaintextTrinoProperties properties, TrinoFileSystemFactory fileSystemFactory)
    {
        GlueClientConfig config;
        try (ConfigurationFactory configFactory = new ConfigurationFactory(properties.properties())) {
            config = configFactory.build(GlueClientConfig.class);
        }
        GlueClient glueClient = glueClientFactoryProvider.get().createFromProperties(config);
        FileIO fileIo = new ForwardingFileIo(
                fileSystemFactory.create(ConnectorIdentity.ofUser("icehouse-maintenance")),
                true);
        return new GlueIcehouseCatalog(glueClient, fileIo, config.isSkipArchive(), typeManager);
    }
}
