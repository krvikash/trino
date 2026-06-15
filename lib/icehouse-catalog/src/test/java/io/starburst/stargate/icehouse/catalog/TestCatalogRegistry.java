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
package io.starburst.stargate.icehouse.catalog;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.icehouse.spi.TableIdentifier;
import io.starburst.stargate.icehouse.spi.maintenance.PlaintextTrinoProperties;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.trino.filesystem.TrinoFileSystemFactory;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCatalogRegistry
{
    @Test
    void registryLooksUpFactoryByKind()
    {
        StubFactory glueFactory = new StubFactory(CatalogKind.GLUE);

        CatalogRegistry registry = new CatalogRegistry(ImmutableSet.of(glueFactory));

        assertThat(registry.registeredKinds()).containsOnly(CatalogKind.GLUE);
        assertThat(registry.factory(CatalogKind.GLUE)).isSameAs(glueFactory);
    }

    @Test
    void registryRejectsLookupForUnregisteredKind()
    {
        CatalogRegistry registry = new CatalogRegistry(ImmutableSet.of());

        assertThatThrownBy(() -> registry.factory(CatalogKind.GLUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No factory registered for catalog kind: GLUE");
    }

    private static final class StubFactory
            implements IcehouseCatalogFactory
    {
        private final CatalogKind kind;

        StubFactory(CatalogKind kind)
        {
            this.kind = kind;
        }

        @Override
        public CatalogKind kind()
        {
            return kind;
        }

        @Override
        public IcehouseCatalog create(AccountId accountId, CatalogId catalogId, PlaintextTrinoProperties properties, TrinoFileSystemFactory fileSystemFactory)
        {
            return new StubCatalog();
        }
    }

    private static final class StubCatalog
            implements IcehouseCatalog
    {
        @Override
        public List<String> listSchemas()
        {
            return List.of();
        }

        @Override
        public List<String> listTables(String schema)
        {
            return List.of();
        }

        @Override
        public Table loadTable(TableIdentifier tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String metadataLocation(TableIdentifier tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
