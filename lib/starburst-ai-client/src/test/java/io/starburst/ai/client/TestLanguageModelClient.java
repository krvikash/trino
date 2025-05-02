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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.starburst.ai.client.bedrock.AwsBedrockLanguageModelClient;
import io.starburst.ai.client.openai.OpenAiLanguageModelClient;
import io.trino.spi.TrinoException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanguageModelClient
{
    private ModelClientProvider modelClientProvider;

    @BeforeAll
    public void setup()
            throws IOException
    {
        modelClientProvider = staticModelClientProvider(LANGUAGE_MODEL_PROVIDERS);
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testPrompt(String modelId)
    {
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).generate(prompt);
        assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paris");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testPromptSystem(String modelId)
    {
        String prompt = """
                You are an expert cartographer and know the capital of each country.
                The user will supply a country name, reply only with the name of the capital city.
                Do not reply with extraneous text.

                If there is any input that does not match a country name,
                please correct the user with the exact text "kindly supply a country name and only a country name".

                Important! If the capital city happens to be Paris, please refer to it as Paname.
                """;
        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).generate(prompt, "France");
        assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paname");

        String incorrectInputResult = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).generate(prompt, "hamburgers");
        assertThat(incorrectInputResult.toLowerCase(ENGLISH).strip()).isEqualTo("kindly supply a country name and only a country name");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testEmptyPrompt(String modelId)
    {
        // This test highlights differences in behavior of supplying an empty prompt.
        // As we add more clients, they should be included in this test
        // Downstream users of these clients may choose to normalize this behavior.
        LanguageModelClient client = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId));
        switch (client) {
            case OpenAiLanguageModelClient openAiClient -> assertThat(openAiClient.generate("")).isNotBlank();
            case AwsBedrockLanguageModelClient awsAiClient -> assertThatThrownBy(() -> awsAiClient.generate(""))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Failed to execute AI request");
            default -> throw new UnsupportedOperationException("Unknown client");
        }
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testExtractPrompt(String modelId)
    {
        String systemPrompt = """
                Extract the a list of values for each of the JSON encoded labels from the text below. For each label, extract all the values into a list
                Labels: %s
                Output the extracted values as a JSON object. Output only the JSON. Do not output a code block for the JSON.
                =====
                %s
                """;

        String text = """
                France has several major cities including Paris, Lyon, Marseille, and Nice.
                The official languages in France are French and several regional languages.
                Popular French foods include croissants, baguettes, and escargot.
                """;
        String labels = """
                ["cities", "languages", "foods"]
                """;

        // Format system prompt with labels first
        String formattedPrompt = systemPrompt.formatted(labels, "%s");

        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).generate(text, formattedPrompt);
        JsonCodec<Map<String, List<String>>> resultCodec = mapJsonCodec(String.class, listJsonCodec(String.class));
        Map<String, List<String>> resultMap = resultCodec.fromJson(result);

        Assertions.assertThat(resultMap).containsKey("cities")
                .satisfies(map -> Assertions.assertThat(map.get("cities").stream()
                        .map(city -> city.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("paris", "lyon", "marseille", "nice"));

        Assertions.assertThat(resultMap).containsKey("languages")
                .satisfies(map -> Assertions.assertThat(map.get("languages").stream()
                        .map(language -> language.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("french"));

        Assertions.assertThat(resultMap).containsKey("foods")
                .satisfies(map -> Assertions.assertThat(map.get("foods").stream()
                        .map(food -> food.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("croissants", "baguettes", "escargot"));
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testClassify(String modelId)
    {
        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).classify("I love this product!", ImmutableList.of("positive", "negative", "neutral"));
        assertThat(result).contains("positive");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMask(String modelId)
    {
        String prompt = "My credit card number is 1234-5678-9012-3456 and my password is hunter2";
        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).mask(prompt, ImmutableList.of("credit card number", "password"));
        assertThat(result.strip()).isEqualTo("My credit card number is [MASKED] and my password is [MASKED]");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testTranslate(String modelId)
    {
        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).translate("Hello world", "Spanish");
        Pattern pattern = Pattern.compile("hola\\s+.*mundo.*");
        assertThat(sanitize(result)).matches(pattern);
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testSummarize(String modelId)
    {
        String prompt = """
               The Amazon rainforest, often referred to as the “lungs of the Earth,” produces around 20% of the world’s oxygen and is home to an estimated 10% of all known species. Despite its crucial ecological role, it faces severe threats from deforestation driven by logging, agriculture, and mining. The loss of forest cover not only endangers biodiversity but also contributes to climate change by releasing massive amounts of carbon dioxide into the atmosphere.
               In addition to its environmental importance, the Amazon plays a critical role in regulating global and regional weather patterns. The vast canopy of trees helps recycle moisture through a process known as transpiration, which in turn influences rainfall across South America and even affects weather as far away as North America and Africa. Disruption of this cycle due to forest loss can lead to more droughts, unpredictable weather, and changes in agricultural productivity across the continent.
               Local and Indigenous communities who have lived in the Amazon for centuries also suffer the consequences of deforestation. Their traditional ways of life are intimately connected to the health of the forest, and many depend on it for food, medicine, and cultural practices. As land is cleared and industrial operations expand, these communities are often displaced or face conflict over land rights and access to natural resources.
               Efforts to protect the Amazon include government regulations, international agreements, and conservation programs run by NGOs and local groups. However, enforcement remains inconsistent, and economic pressures often outweigh environmental considerations. Without stronger global cooperation and sustainable economic alternatives, the Amazon may soon reach a tipping point beyond which it cannot recover—threatening not just regional stability, but the global climate system.""";

        String result = modelClientProvider.languageModelClient(Slices.utf8Slice(modelId)).summarize(prompt);
        assertThat(sanitize(result))
                .contains("rainforest", "deforestation")
                .hasSizeLessThan(prompt.length());
    }

    private static String sanitize(String input)
    {
        return input
                .replaceAll("[^a-zA-Z]", " ")
                .toLowerCase(ENGLISH)
                .strip();
    }

    public static Object[][] modelIds()
    {
        return new Object[][] {
                {"haiku35"},
                {"gpt4o_mini"},
                {"meta_llama"}
        };
    }
}
