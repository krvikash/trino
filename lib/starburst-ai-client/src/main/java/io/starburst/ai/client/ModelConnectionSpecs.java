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
package io.starburst.ai.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ModelConnectionSpecs
{
    public static final ModelConnectionSpecs EMPTY_SPECS = new ModelConnectionSpecs(ImmutableList.of());

    private final Map<String, LanguageModelConnectionSpec> languageModelConnectionSpecs;
    private final Map<String, EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs;
    private List<ModelConnectionSpec> models;

    @JsonCreator
    public ModelConnectionSpecs(@JsonProperty List<ModelConnectionSpec> models)
    {
        this.models = requireNonNull(models, "models is null");
        ImmutableMap.Builder<String, LanguageModelConnectionSpec> languageModelBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, EmbeddingModelConnectionSpec> embeddingModelBuilder = ImmutableMap.builder();

        models.forEach(spec -> {
            switch (spec) {
                case LanguageModelConnectionSpec languageModelSpec -> languageModelBuilder.put(languageModelSpec.id(), languageModelSpec);
                case EmbeddingModelConnectionSpec embeddingModelSpec -> embeddingModelBuilder.put(embeddingModelSpec.id(), embeddingModelSpec);
            }});
        this.languageModelConnectionSpecs = languageModelBuilder.buildOrThrow();
        this.embeddingModelConnectionSpecs = embeddingModelBuilder.buildOrThrow();
    }

    @JsonProperty
    public List<ModelConnectionSpec> models()
    {
        return models;
    }

    public Collection<LanguageModelConnectionSpec> languageModelConnectionSpecs()
    {
        return languageModelConnectionSpecs.values();
    }

    public Optional<LanguageModelConnectionSpec> getLanguageModelConnectionSpecById(String id)
    {
        return Optional.ofNullable(languageModelConnectionSpecs.get(id));
    }

    public Collection<EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs()
    {
        return embeddingModelConnectionSpecs.values();
    }

    public Optional<EmbeddingModelConnectionSpec> getEmbeddingModelConnectionSpecById(String id)
    {
        return Optional.ofNullable(embeddingModelConnectionSpecs.get(id));
    }
}
