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

import com.google.inject.Inject;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.toImmutableEnumMap;
import static java.util.function.Function.identity;

/**
 * Kind → {@link CatalogPropertyTranslator} lookup. Coordinator-only.
 */
public final class CatalogPropertyTranslatorRegistry
{
    private final Map<CatalogKind, CatalogPropertyTranslator> translators;

    @Inject
    public CatalogPropertyTranslatorRegistry(Set<CatalogPropertyTranslator> translatorSet)
    {
        this.translators = translatorSet.stream()
                .collect(toImmutableEnumMap(CatalogPropertyTranslator::kind, identity()));
    }

    public CatalogPropertyTranslator translator(CatalogKind kind)
    {
        CatalogPropertyTranslator translator = translators.get(kind);
        checkArgument(translator != null, "No property translator registered for catalog kind: %s", kind);
        return translator;
    }

    public Set<CatalogKind> registeredKinds()
    {
        return translators.keySet();
    }
}
