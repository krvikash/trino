/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
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
