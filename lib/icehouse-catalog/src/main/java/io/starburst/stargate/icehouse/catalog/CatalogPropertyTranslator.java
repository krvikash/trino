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

import java.util.Map;

/**
 * Translates Galaxy's unsealed catalog attributes into the Trino property map
 * the backend's catalog client expects. One implementation per
 * {@link CatalogKind}, registered via Guice multibinder.
 */
public interface CatalogPropertyTranslator
{
    CatalogKind kind();

    PlaintextTrinoProperties translate(AccountId accountId, Map<String, String> unsealedAttributes);
}
