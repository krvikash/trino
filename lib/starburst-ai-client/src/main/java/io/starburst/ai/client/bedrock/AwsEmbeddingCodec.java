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
import io.airlift.slice.Slice;
import io.starburst.ai.client.EmbeddingType;
import io.starburst.ai.model.EmbeddingModelConnectionSpec;

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
