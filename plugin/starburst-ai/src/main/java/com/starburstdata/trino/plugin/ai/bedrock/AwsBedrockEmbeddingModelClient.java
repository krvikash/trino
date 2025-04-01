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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.starburstdata.trino.plugin.ai.AiErrorCode;
import com.starburstdata.trino.plugin.ai.EmbeddingModelClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
            builder.body(SdkBytes.fromUtf8String(embeddingCodec.generateRequestBody(stripWhitespace(sourceString.toStringUtf8()))));
            builder.modelId(modelName);
        });

        try {
            JsonNode responseBody = OBJECT_MAPPER.readTree(response.body().asInputStream());
            return embeddingCodec.parseResponse(responseBody);
        }
        catch (IOException e) {
            throw new TrinoException(AiErrorCode.AI_ERROR, "Failed to read response from embedding model", e);
        }
    }

    @Override
    public List<List<Float>> generateEmbeddings(List<Slice> sourceStrings)
    {
        Iterator<String> requests = embeddingCodec.generateBatchRequestBodies(Iterators.transform(sourceStrings.iterator(), slice -> stripWhitespace(slice.toStringUtf8())));
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
                throw new TrinoException(AiErrorCode.AI_ERROR, "Failed to read response from embedding model", e);
            }
        }

        return embeddings.build();
    }

    private static String stripWhitespace(String value)
    {
        return value.replaceAll("[\n\t\r\b\f\0\"\\\\]", " ");
    }
}
