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
 * Abstracts catalog DDL side-effects that differ between Galaxy-managed and
 * standalone SEP deployments.  Galaxy mode needs no local DB work beyond what
 * stargate already handles; SEP mode must additionally record the Galaxy-specific
 * data (e.g., account ID, ownership record, RBAC grants)
 *
 * @see GalaxyCatalogDdlObserver
 * @see StacCatalogDdlObserver
 */
public interface CatalogDdlObserver
{
    /**
     * Called after the catalog-created event has been forwarded to stargate.
     * Implementations may perform additional local bookkeeping required by the
     * deployment mode.
     */
    void onCatalogCreated(DispatchSession session, CatalogId catalogId);

    /**
     * Called after the catalog-dropped event has been forwarded to stargate.
     * Implementations may perform additional local cleanup required by the
     * deployment mode.
     */
    void onCatalogDropped(DispatchSession session, CatalogId catalogId);
}
