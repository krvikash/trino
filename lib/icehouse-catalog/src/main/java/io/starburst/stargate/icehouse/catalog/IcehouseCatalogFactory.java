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

import io.starburst.stargate.icehouse.spi.maintenance.PlaintextTrinoProperties;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.trino.filesystem.TrinoFileSystemFactory;

/**
 * Per-kind factory for short-lived {@link IcehouseCatalog} handles. Registered
 * via Guice multibinder and collected by {@link CatalogRegistry}.
 */
public interface IcehouseCatalogFactory
{
    CatalogKind kind();

    IcehouseCatalog create(
            AccountId accountId,
            CatalogId catalogId,
            PlaintextTrinoProperties properties,
            TrinoFileSystemFactory fileSystemFactory);
}
