/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;

import static io.starburst.ai.client.TestingUtils.EMBEDDING_MODEL_PROVIDERS;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestEmbeddingModelClient
{
    private ModelClientProvider modelClientProvider;

    @BeforeAll
    public void setup()
            throws IOException
    {
        modelClientProvider = staticModelClientProvider(EMBEDDING_MODEL_PROVIDERS);
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testGenerateEmbeddings(String modelId)
    {
        List<List<Float>> results = modelClientProvider.embeddingModelClient(Slices.utf8Slice(modelId)).generateEmbeddings(ImmutableList.of(
                Slices.utf8Slice("apple"),
                Slices.utf8Slice("apple"),
                Slices.utf8Slice("orange"),
                Slices.utf8Slice("cat"),
                Slices.utf8Slice("dog")));

        assertThat(results).hasSize(5);
        // The size differs for each model but they should all produce at least 100 embeddings
        assertThat(results)
                .satisfies(list -> assertThat(list).allMatch(sublist -> sublist.size() == list.get(0).size() && sublist.size() > 100));
    }

    public static Object[][] modelIds()
    {
        return new Object[][] {
                {"openai_embed_3_small"},
                {"openai_embed_3_large"},
                {"titan_v2"},
                {"cohere_3_multi"},
        };
    }
}
