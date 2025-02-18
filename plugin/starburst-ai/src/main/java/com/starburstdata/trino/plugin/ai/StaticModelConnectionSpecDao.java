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
package com.starburstdata.trino.plugin.ai;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class StaticModelConnectionSpecDao
        implements ModelConnectionSpecDao
{
    private final Map<String, LanguageModelConnectionSpec> languageModelConnectionSpecs;
    private final Map<String, EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs;

    @Inject
    public StaticModelConnectionSpecDao(List<ModelConnectionSpec> modelConnectionSpecs)
    {
        requireNonNull(modelConnectionSpecs, "models is null");
        ImmutableMap.Builder<String, LanguageModelConnectionSpec> languageModelBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, EmbeddingModelConnectionSpec> embeddingModelBuilder = ImmutableMap.builder();

        modelConnectionSpecs.forEach(spec -> {
            switch (spec) {
                case LanguageModelConnectionSpec languageModelSpec -> languageModelBuilder.put(languageModelSpec.id(), languageModelSpec);
                case EmbeddingModelConnectionSpec embeddingModelSpec -> embeddingModelBuilder.put(embeddingModelSpec.id(), embeddingModelSpec);
            }});
        this.languageModelConnectionSpecs = languageModelBuilder.buildOrThrow();
        this.embeddingModelConnectionSpecs = embeddingModelBuilder.buildOrThrow();
    }

    @Override
    public Collection<LanguageModelConnectionSpec> languageModelConnectionSpecs()
    {
        return languageModelConnectionSpecs.values();
    }

    @Override
    public LanguageModelConnectionSpec getLanguageModelConnectionSpecById(String id)
    {
        LanguageModelConnectionSpec spec = languageModelConnectionSpecs.get(id);
        if (spec == null) {
            throw new TrinoException(NOT_FOUND, "Language model connection spec not found for id: %s".formatted(id));
        }
        return spec;
    }

    @Override
    public Collection<EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs()
    {
        return embeddingModelConnectionSpecs.values();
    }

    @Override
    public EmbeddingModelConnectionSpec getEmbeddingModelConnectionSpecById(String id)
    {
        EmbeddingModelConnectionSpec spec = embeddingModelConnectionSpecs.get(id);
        if (spec == null) {
            throw new TrinoException(NOT_FOUND, "Embedding model connection spec not found for id: %s".formatted(id));
        }
        return spec;
    }
}
