/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.openai.client.OpenAIClient;
import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.Embedding;
import com.openai.models.embeddings.EmbeddingCreateParams;
import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingModelClient;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class OpenAiEmbeddingModelClient
        implements EmbeddingModelClient
{
    private static final int BATCH_SIZE = 2048;

    private final String modelName;
    private final Optional<Integer> dimensions;
    private final OpenAIClient client;

    public OpenAiEmbeddingModelClient(String modelName, Optional<Integer> dimensions, OpenAIClient client)
    {
        this.modelName = modelName;
        this.dimensions = dimensions;
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<Double> generateEmbedding(Slice sourceString)
    {
        String value = sourceString.toStringUtf8();
        EmbeddingCreateParams.Builder params = EmbeddingCreateParams.builder()
                .input(value)
                .model(modelName);
        dimensions.ifPresent(params::dimensions);
        CreateEmbeddingResponse response = client.embeddings().create(params.build());
        return getOnlyElement(response.data()).embedding().stream().map(Float::doubleValue).collect(toImmutableList());
    }

    @Override
    public List<List<Float>> generateEmbeddings(List<Slice> sourceStrings)
    {
        ImmutableList.Builder<List<Float>> results = ImmutableList.builder();
        for (List<Slice> sourceStringsBatch : Lists.partition(sourceStrings, BATCH_SIZE)) {
            List<String> values = sourceStringsBatch.stream().map(Slice::toStringUtf8).toList();
            EmbeddingCreateParams.Builder params = EmbeddingCreateParams.builder()
                    .inputOfArrayOfStrings(values)
                    .model(modelName);
            dimensions.ifPresent(params::dimensions);
            CreateEmbeddingResponse response = client.embeddings().create(params.build());
            response.data().stream()
                    .map(Embedding::embedding)
                    .forEach(results::add);
        }

        return results.build();
    }
}
