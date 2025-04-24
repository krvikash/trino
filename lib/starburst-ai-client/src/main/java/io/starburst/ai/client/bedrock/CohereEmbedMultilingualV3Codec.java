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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.starburst.ai.client.EmbeddingModelConnectionSpec;
import io.starburst.ai.client.EmbeddingType;
import io.trino.spi.TrinoException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_SPEC_PROPERTY;
import static java.util.Objects.requireNonNull;

public final class CohereEmbedMultilingualV3Codec
        implements AwsEmbeddingCodec
{
    public static final String MODEL_NAME = "cohere.embed-multilingual-v3";

    private static final int BATCH_SIZE = 96;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final Map<EmbeddingType, String> EMBEDDING_TYPE_API_VALUES = ImmutableMap.<EmbeddingType, String>builder()
            .put(EmbeddingType.FLOAT, "float")
            .put(EmbeddingType.BINARY, "binary")
            .buildOrThrow();

    private static final String COHERE_REQUEST_TEMPLATE = """
            {
                "texts":["%s"],
                "input_type": "search_query",
                "truncate": "NONE",
                "embedding_types": ["%s"]
            }
            """;
    private static final String COHERE_BATCH_REQUEST_TEMPLATE = """
                {
                    "texts":%s,
                    "input_type": "search_document",
                    "truncate": "NONE",
                    "embedding_types": ["%s"]
                }
                """;

    private CohereEmbedMultilingualV3Codec() {}

    public static class Factory
            implements AwsEmbeddingCodec.Factory
    {
        @Override
        public AwsEmbeddingCodec create(EmbeddingModelConnectionSpec spec)
        {
            requireNonNull(spec, "spec is null");
            requireNonNull(spec.dimensions(), "dimensions is null");
            if (spec.dimensions().isPresent()) {
                throw new TrinoException(INVALID_MODEL_SPEC_PROPERTY, "The Cohere embed 3 multilingual embedding model does not support setting a dimension size.");
            }

            return new CohereEmbedMultilingualV3Codec();
        }
    }

    @Override
    public String generateRequestBody(String sourceString, EmbeddingType embeddingType)
    {
        return COHERE_REQUEST_TEMPLATE.formatted(sourceString, EMBEDDING_TYPE_API_VALUES.get(embeddingType));
    }

    @Override
    public Iterator<String> generateBatchRequestBodies(Iterator<String> sourceStrings, EmbeddingType embeddingType)
    {
        Iterator<List<String>> batches = Iterators.partition(sourceStrings, BATCH_SIZE);
        return Iterators.transform(batches, batch -> {
            try {
                return COHERE_BATCH_REQUEST_TEMPLATE.formatted(OBJECT_MAPPER.writeValueAsString(batch), EMBEDDING_TYPE_API_VALUES.get(embeddingType));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public List<Double> parseResponse(JsonNode responseBody)
    {
        ImmutableList.Builder<Double> elements = ImmutableList.builder();
        for (JsonNode doubleValue : responseBody.get("embeddings").get("float").get(0)) {
            elements.add(doubleValue.asDouble());
        }
        return elements.build();
    }

    @Override
    public Slice parseBinaryResponse(JsonNode jsonNode)
    {
        // Cohere responds with a list of int8
        byte[] embedding = new byte[128];
        JsonNode elements = jsonNode.get("embeddings").get("binary").get(0);
        if (elements.size() != 128) {
            throw new TrinoException(AI_CLIENT_ERROR, "Cohere response had an unexpected dimension count: " + elements.size());
        }

        for (int i = 0; i < 128; i++) {
            embedding[i] = (byte) elements.get(i).asInt();
        }
        return Slices.wrappedBuffer(embedding, 0, 128);
    }

    @Override
    public List<List<Float>> parseBatchResponse(JsonNode responseBody)
    {
        ImmutableList.Builder<List<Float>> embeddings = ImmutableList.builder();
        for (JsonNode embedding : responseBody.get("embeddings").get("float")) {
            ImmutableList.Builder<Float> elements = ImmutableList.builder();
            for (JsonNode floatValue : embedding) {
                elements.add(floatValue.floatValue());
            }
            embeddings.add(elements.build());
        }
        return embeddings.build();
    }

    @Override
    public List<Slice> parseBinaryBatchResponse(JsonNode responseBody)
    {
        // Cohere responds with a list of int8
        JsonNode embeddings = responseBody.get("embeddings").get("binary");
        ImmutableList.Builder<Slice> results = ImmutableList.builder();
        for (JsonNode entry : embeddings) {
            byte[] embedding = new byte[128];
            if (entry.size() != 128) {
                throw new TrinoException(AI_CLIENT_ERROR, "Cohere response had an unexpected dimension count: " + entry.size());
            }

            for (int i = 0; i < 128; i++) {
                embedding[i] = (byte) entry.get(i).asInt();
            }
            results.add(Slices.wrappedBuffer(embedding, 0, 128));
        }
        return results.build();
    }
}
