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
import com.starburstdata.trino.plugin.ai.EmbeddingModelConnectionSpec;
import com.starburstdata.trino.plugin.ai.EmbeddingType;
import io.airlift.slice.Slice;

import java.util.Iterator;
import java.util.List;

public interface AwsEmbeddingCodec
{
    String generateRequestBody(String sourceString, EmbeddingType embeddingType);

    Iterator<String> generateBatchRequestBodies(Iterator<String> sourceStrings, EmbeddingType embeddingType);

    List<Double> parseResponse(JsonNode responseBody);

    Slice parseBinaryResponse(JsonNode responseBody);

    List<List<Float>> parseBatchResponse(JsonNode responseBody);

    List<Slice> parseBinaryBatchResponse(JsonNode responseBody);

    interface Factory
    {
        AwsEmbeddingCodec create(EmbeddingModelConnectionSpec spec);
    }
}
