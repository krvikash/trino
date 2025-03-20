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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.starburstdata.trino.plugin.ai.EmbeddingModelConnectionSpec;
import com.starburstdata.trino.plugin.ai.EmbeddingType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.starburstdata.trino.plugin.ai.AiErrorCode.INVALID_MODEL_SPEC_PROPERTY;
import static java.util.Objects.requireNonNull;

public final class TitanTextV2Codec
        implements AwsEmbeddingCodec
{
    public static final String MODEL_NAME = "amazon.titan-embed-text-v2:0";

    private static final String TITAN_REQUEST_TEMPLATE = """
            {
                "inputText": "%s",
                "dimensions": %d,
                "normalize": true,
                "embeddingTypes": ["%s"]
            }
            """;
    private static final String TITAN_DEFAULT_DIMENSIONALITY_REQUEST_TEMPLATE = """
            {
                "inputText": "%s",
                "normalize": true,
                "embeddingTypes": ["%s"]
            }
            """;

    private static final Set<Integer> TITAN_VALID_DIMENSION_COUNTS = ImmutableSet.of(1024, 512, 256);

    private final Optional<Integer> dimensions;

    private TitanTextV2Codec(Optional<Integer> dimensions)
    {
        this.dimensions = dimensions;
    }

    public static class Factory
            implements AwsEmbeddingCodec.Factory
    {
        @Override
        public AwsEmbeddingCodec create(EmbeddingModelConnectionSpec spec)
        {
            requireNonNull(spec, "spec is null");
            requireNonNull(spec.dimensions(), "dimensions is null");
            if (spec.dimensions().map(d -> !TITAN_VALID_DIMENSION_COUNTS.contains(d)).orElse(false)) {
                throw new TrinoException(INVALID_MODEL_SPEC_PROPERTY, "The Titan text embedding model requires dimension count be one of: %s".formatted(TITAN_VALID_DIMENSION_COUNTS));
            }
            return new TitanTextV2Codec(spec.dimensions());
        }
    }

    @Override
    public String generateRequestBody(String sourceString, EmbeddingType embeddingType)
    {
        return dimensions
                .map(d -> TITAN_REQUEST_TEMPLATE.formatted(sourceString, d, embeddingType.toString().toLowerCase(Locale.ENGLISH)))
                .orElse(TITAN_DEFAULT_DIMENSIONALITY_REQUEST_TEMPLATE.formatted(sourceString, embeddingType.toString().toLowerCase(Locale.ENGLISH)));
    }

    @Override
    public Iterator<String> generateBatchRequestBodies(Iterator<String> sourceStrings, EmbeddingType embeddingType)
    {
        return Iterators.transform(sourceStrings, source -> generateRequestBody(source, embeddingType));
    }

    @Override
    public List<Double> parseResponse(JsonNode responseBody)
    {
        ImmutableList.Builder<Double> elements = ImmutableList.builder();
        for (JsonNode doubleValue : responseBody.get("embedding")) {
            elements.add(doubleValue.asDouble());
        }
        return elements.build();
    }

    @Override
    public Slice parseBinaryResponse(JsonNode jsonNode)
    {
        // Titan responds with a list of boolean values, as 1s or 0s.
        // Dimension count is always one of [1024, 512, 256]
        JsonNode elements = jsonNode.get("embeddingsByType").get("binary");
        int byteEncodingLength = elements.size() / 8;
        byte[] embedding = new byte[byteEncodingLength];
        int position = 0;
        for (int i = 0; i < byteEncodingLength; i++) {
            int element = 0;
            for (int bit = 0; bit < 8; bit++) {
                element = (element << 1) | elements.get(position).asInt();
                position++;
            }
            embedding[i] = (byte) element;
        }
        return Slices.wrappedBuffer(embedding, 0, byteEncodingLength);
    }

    @Override
    public List<List<Float>> parseBatchResponse(JsonNode responseBody)
    {
        return List.of(parseResponse(responseBody).stream().map(Double::floatValue).toList());
    }

    @Override
    public List<Slice> parseBinaryBatchResponse(JsonNode responseBody)
    {
        return List.of(parseBinaryResponse(responseBody));
    }
}
