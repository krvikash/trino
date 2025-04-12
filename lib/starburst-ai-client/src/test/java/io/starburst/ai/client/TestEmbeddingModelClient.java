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
