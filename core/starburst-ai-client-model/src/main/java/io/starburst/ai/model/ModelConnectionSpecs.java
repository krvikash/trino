/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ModelConnectionSpecs
{
    public static final ModelConnectionSpecs EMPTY_SPECS = new ModelConnectionSpecs(List.of());

    private final Map<String, LanguageModelConnectionSpec> languageModelConnectionSpecs;
    private final Map<String, EmbeddingModelConnectionSpec> embeddingModelConnectionSpecs;
    private final List<ModelConnectionSpec> models;

    @JsonCreator
    public ModelConnectionSpecs(@JsonProperty List<ModelConnectionSpec> models)
    {
        this.models = requireNonNull(models, "models is null");

        this.languageModelConnectionSpecs = models.stream()
                .filter(LanguageModelConnectionSpec.class::isInstance)
                .map(LanguageModelConnectionSpec.class::cast)
                .collect(Collectors.toUnmodifiableMap(LanguageModelConnectionSpec::id, Function.identity()));

        this.embeddingModelConnectionSpecs = models.stream()
                .filter(EmbeddingModelConnectionSpec.class::isInstance)
                .map(EmbeddingModelConnectionSpec.class::cast)
                .collect(Collectors.toUnmodifiableMap(ModelConnectionSpec::id, Function.identity()));
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
