/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingType;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static java.util.Objects.requireNonNull;

public class AwsBedrockEmbeddingModelClient
        implements EmbeddingModelClient
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final BedrockRuntimeClient bedrockRuntimeClient;
    private final String modelName;
    private final AwsEmbeddingCodec embeddingCodec;

    public AwsBedrockEmbeddingModelClient(
            String modelName,
            AwsEmbeddingCodec embeddingCodec,
            BedrockRuntimeClient client)
    {
        this.modelName = requireNonNull(modelName, "modelName is null");
        this.embeddingCodec = requireNonNull(embeddingCodec, "embeddingCodec is null");
        this.bedrockRuntimeClient = requireNonNull(client, "client is null");
    }

    @Override
    public List<Double> generateEmbedding(Slice sourceString)
    {
        InvokeModelResponse response = bedrockRuntimeClient.invokeModel(builder -> {
            builder.body(SdkBytes.fromUtf8String(embeddingCodec.generateRequestBody(stripWhitespace(sourceString.toStringUtf8()), EmbeddingType.FLOAT)));
            builder.modelId(modelName);
        });

        try {
            JsonNode responseBody = OBJECT_MAPPER.readTree(response.body().asInputStream());
            return embeddingCodec.parseResponse(responseBody);
        }
        catch (IOException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to read response from embedding model", e);
        }
    }

    @Override
    public List<List<Float>> generateEmbeddings(List<Slice> sourceStrings)
    {
        Iterator<String> requests = embeddingCodec.generateBatchRequestBodies(
                Iterators.transform(sourceStrings.iterator(), slice -> stripWhitespace(slice.toStringUtf8())),
                EmbeddingType.FLOAT);
        ImmutableList.Builder<List<Float>> embeddings = ImmutableList.builder();
        while (requests.hasNext()) {
            String requestBody = requests.next();
            InvokeModelResponse response = bedrockRuntimeClient.invokeModel(builder -> {
                builder.body(SdkBytes.fromUtf8String(requestBody));
                builder.modelId(modelName);
            });

            try {
                JsonNode responseBody = OBJECT_MAPPER.readTree(response.body().asInputStream());
                embeddings.addAll(embeddingCodec.parseBatchResponse(responseBody));
            }
            catch (IOException e) {
                throw new TrinoException(AI_CLIENT_ERROR, "Failed to read response from embedding model", e);
            }
        }

        return embeddings.build();
    }

    @Override
    public Slice generateBinaryEmbedding(Slice sourceString)
    {
        InvokeModelResponse response = bedrockRuntimeClient.invokeModel(builder -> {
            builder.body(SdkBytes.fromUtf8String(embeddingCodec.generateRequestBody(stripWhitespace(sourceString.toStringUtf8()), EmbeddingType.BINARY)));
            builder.modelId(modelName);
        });

        try {
            JsonNode responseBody = OBJECT_MAPPER.readTree(response.body().asInputStream());
            return embeddingCodec.parseBinaryResponse(responseBody);
        }
        catch (IOException e) {
            throw new TrinoException(AI_CLIENT_ERROR, "Failed to read response from embedding model", e);
        }
    }

    @Override
    public List<Slice> generateBinaryEmbeddings(List<Slice> sourceStrings)
    {
        Iterator<String> requests = embeddingCodec.generateBatchRequestBodies(
                Iterators.transform(sourceStrings.iterator(), slice -> stripWhitespace(slice.toStringUtf8())),
                EmbeddingType.BINARY);
        ImmutableList.Builder<Slice> embeddings = ImmutableList.builder();
        while (requests.hasNext()) {
            String requestBody = requests.next();
            InvokeModelResponse response = bedrockRuntimeClient.invokeModel(builder -> {
                builder.body(SdkBytes.fromUtf8String(requestBody));
                builder.modelId(modelName);
            });

            try {
                JsonNode responseBody = OBJECT_MAPPER.readTree(response.body().asInputStream());
                embeddings.addAll(embeddingCodec.parseBinaryBatchResponse(responseBody));
            }
            catch (IOException e) {
                throw new TrinoException(AI_CLIENT_ERROR, "Failed to read response from embedding model", e);
            }
        }

        return embeddings.build();
    }

    private static String stripWhitespace(String value)
    {
        return value.replaceAll("[\n\t\r\b\f\0\"\\\\]", " ");
    }
}
