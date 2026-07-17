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
package io.trino.server.starburst.accesscontrol;

import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.identity.DispatchSession;

/**
 * Galaxy-mode stub: catalog lifecycle in Galaxy is managed at the platform level
 * and never flows through the Trino DDL path, so these methods should never be called.
 */
public class GalaxyCatalogDdlObserver
        implements CatalogDdlObserver
{
    @Override
    public void onCatalogCreated(DispatchSession session, CatalogId catalogId)
    {
        throw new UnsupportedOperationException("Catalog DDL lifecycle is not managed via Trino in Galaxy mode");
    }

    @Override
    public void onCatalogDropped(DispatchSession session, CatalogId catalogId)
    {
        throw new UnsupportedOperationException("Catalog DDL lifecycle is not managed via Trino in Galaxy mode");
    }
}
