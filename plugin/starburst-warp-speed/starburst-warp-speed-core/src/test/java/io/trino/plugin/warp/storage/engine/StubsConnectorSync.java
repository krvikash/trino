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
package io.trino.plugin.warp.storage.engine;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.lang.foreign.Arena;

@Singleton
public class StubsConnectorSync
        implements ConnectorSync
{
    @Inject
    public StubsConnectorSync() {}

    @Override
    public long getCatalogContext()
    {
        return 0;
    }

    @Override
    public boolean isDefaultCatalog()
    {
        return true;
    }

    @Override
    public boolean isCatalogReducedResources()
    {
        return false;
    }

    @Override
    public QueryMemory allocQueryMemory()
    {
        return new QueryMemory(0, Arena.ofAuto().allocate(1024 * 1024, 4));
    }

    @Override
    public void freeQueryMemory(int queryMemoryId) {}
}
