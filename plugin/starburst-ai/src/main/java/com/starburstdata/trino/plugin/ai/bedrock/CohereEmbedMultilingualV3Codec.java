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
package com.starburstdata.trino.plugin.ai.bedrock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.starburstdata.trino.plugin.ai.EmbeddingModelConnectionSpec;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.TrinoException;

import java.util.Iterator;
import java.util.List;

import static com.starburstdata.trino.plugin.ai.AiErrorCode.INVALID_MODEL_SPEC_PROPERTY;
import static java.util.Objects.requireNonNull;

public final class CohereEmbedMultilingualV3Codec
        implements AwsEmbeddingCodec
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final int BATCH_SIZE = 96;
    public static final String MODEL_NAME = "cohere.embed-multilingual-v3";

    private static final String COHERE_REQUEST_TEMPLATE = """
            {
                "texts":["%s"],
                "input_type": "search_query",
                "truncate": "NONE",
                "embedding_types": ["float"]
            }
            """;
    private static final String COHERE_BATCH_REQUEST_TEMPLATE = """
                {
                    "texts":%s,
                    "input_type": "search_document",
                    "truncate": "NONE",
                    "embedding_types": ["float"]
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
    public String generateRequestBody(String sourceString)
    {
        return COHERE_REQUEST_TEMPLATE.formatted(sourceString);
    }

    @Override
    public Iterator<String> generateBatchRequestBodies(Iterator<String> sourceStrings)
    {
        Iterator<List<String>> batches = Iterators.partition(sourceStrings, BATCH_SIZE);
        return Iterators.transform(batches, batch -> {
            try {
                return COHERE_BATCH_REQUEST_TEMPLATE.formatted(OBJECT_MAPPER.writeValueAsString(batch));
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
}
