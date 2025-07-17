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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Optional;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "kind")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LanguageModelConnectionSpec.class, name = "GENERATE"),
        @JsonSubTypes.Type(value = EmbeddingModelConnectionSpec.class, name = "EMBED"),
})
public sealed interface ModelConnectionSpec
        permits LanguageModelConnectionSpec, EmbeddingModelConnectionSpec
{
    /**
     * Returns the id used to identify the model in queries.
     *
     * @return id
     */
    String id();

    /**
     * Returns the model name which is used by the model provider.
     *
     * @return modelName
     */
    String modelName();

    ConnectionInfo connectionInfo();

    /**
     * Returns the description of the model.
     *
     * @return description
     */
    Optional<String> description();
}
