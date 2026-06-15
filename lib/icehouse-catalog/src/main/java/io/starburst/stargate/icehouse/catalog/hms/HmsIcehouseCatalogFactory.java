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
package io.starburst.stargate.icehouse.catalog.hms;

import com.google.inject.Provider;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.stargate.icehouse.catalog.CatalogKind;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalog;
import io.starburst.stargate.icehouse.catalog.IcehouseCatalogFactory;
import io.starburst.stargate.icehouse.spi.maintenance.PlaintextTrinoProperties;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.inject.Inject;
import org.apache.iceberg.io.FileIO;

import static java.util.Objects.requireNonNull;

/**
 * Builds an {@link HmsIcehouseCatalog} from already-translated plaintext properties.
 */
public final class HmsIcehouseCatalogFactory
        implements IcehouseCatalogFactory
{
    private final Provider<HmsClientFactory> hmsClientFactoryProvider;
    private final Tracer tracer;

    @Inject
    public HmsIcehouseCatalogFactory(Tracer tracer, Provider<HmsClientFactory> hmsClientFactoryProvider)
    {
        this.hmsClientFactoryProvider = requireNonNull(hmsClientFactoryProvider, "hmsClientFactoryProvider is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public CatalogKind kind()
    {
        return CatalogKind.HIVE_METASTORE;
    }

    @Override
    public IcehouseCatalog create(AccountId accountId, CatalogId catalogId, PlaintextTrinoProperties properties, TrinoFileSystemFactory fileSystemFactory)
    {
        HiveMetastore metastore = hmsClientFactoryProvider.get()
                .createFromProperties(tracer, properties.properties(), fileSystemFactory);
        FileIO fileIo = new ForwardingFileIo(
                fileSystemFactory.create(ConnectorIdentity.ofUser("icehouse-maintenance")),
                true);
        return new HmsIcehouseCatalog(metastore, fileIo);
    }
}
