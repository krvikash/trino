/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai;

import io.airlift.json.JsonCodec;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.trino.plugin.ai.AiQueryRunner.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.ai.AiQueryRunner.addStarburstAiCatalog;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAiFunctions
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<List<LabelAndContent>> LABEL_AND_CONTENT_CODEC = listJsonCodec(LabelAndContent.class);

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(LANGUAGE_MODEL_PROVIDERS, runner))
                .build();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testPrompt(String modelId)
    {
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        String result = (String) computeActual(TEST_AI_SESSION, "SELECT ai.prompt('%s', '%s')".formatted(prompt, modelId)).getOnlyValue();
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
        String result = (String) computeActual(TEST_AI_SESSION, "SELECT ai.prompt('France', '%s', '%s')".formatted(prompt, modelId)).getOnlyValue();
        assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paname");

        String incorrectInputResult = (String) computeActual(TEST_AI_SESSION, "SELECT ai.prompt('hamburgers', '%s', '%s')".formatted(prompt, modelId)).getOnlyValue();
        assertThat(incorrectInputResult.toLowerCase(ENGLISH)).contains("kindly supply a country name and only a country name");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testEmptyPrompt(String modelId)
    {
        assertQuery(TEST_AI_SESSION,
                "SELECT ai.prompt('', '%s')".formatted(modelId),
                "VALUES NULL");
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

        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.prompt('%s', '%s', '%s')".formatted(text, formattedPrompt, modelId)).getOnlyValue();

        JsonCodec<Map<String, List<String>>> resultCodec = mapJsonCodec(String.class, listJsonCodec(String.class));
        Map<String, List<String>> resultMap = resultCodec.fromJson(result);

        assertThat(resultMap).containsKey("cities")
                .satisfies(map -> assertThat(map.get("cities").stream()
                        .map(city -> city.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("paris", "lyon", "marseille", "nice"));

        assertThat(resultMap).containsKey("languages")
                .satisfies(map -> assertThat(map.get("languages").stream()
                        .map(language -> language.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("french"));

        assertThat(resultMap).containsKey("foods")
                .satisfies(map -> assertThat(map.get("foods").stream()
                        .map(food -> food.toLowerCase(ENGLISH))
                        .collect(toImmutableSet())).contains("croissants", "baguettes", "escargot"));
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testClassify(String modelId)
    {
        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(modelId)).getOnlyValue();
        assertThat(result).contains("positive");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testClassifySentimentPrompt(String modelId)
    {
        String systemPrompt = """
                Classify the user text into one of the following JSON encoded labels: [ "positive", "negative", "neutral", "mixed" ]
                The user will input a strict json list, classify each item in the list separately.
                Output json list of objects with label and content.

                The output should be strict json. The entire output will be fed into a json parser and must be valid.
                The user text should be returned exactly as is, no changes.
                Do not include any extraneous header or footer text.
                Do not output a code block for the JSON.
                """;

        String reviews = """
                {"I love this product!", "I hate this product!", "I like the product but it smells bad", "I could take it or leave it"}""";

        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.prompt('%s', '%s', '%s')".formatted(reviews, systemPrompt, modelId)).getOnlyValue();

        List<LabelAndContent> resultList = LABEL_AND_CONTENT_CODEC.fromJson(result);

        assertThat(resultList)
                .contains(new LabelAndContent("positive", "I love this product!"))
                .contains(new LabelAndContent("negative", "I hate this product!"))
                .contains(new LabelAndContent("mixed", "I like the product but it smells bad"))
                .contains(new LabelAndContent("neutral", "I could take it or leave it"));
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMask(String modelId)
    {
        String prompt = "My credit card number is 1234-5678-9012-3456 and my password is hunter2";
        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.mask('%s', ARRAY['credit card number', 'password'], '%s')".formatted(prompt, modelId)).getOnlyValue();
        assertThat(result.strip())
                .isEqualTo("My credit card number is [MASKED] and my password is [MASKED]");
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testTranslate(String modelId)
    {
        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.translate('Hello world', 'Spanish', '%s')".formatted(modelId)).getOnlyValue();

        Pattern pattern = Pattern.compile("hola\\s+.*mundo.*");
        assertThat(sanitize(result)).matches(pattern);

        result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.translate('Hello world', 'German', '%s')".formatted(modelId)).getOnlyValue();
        pattern = Pattern.compile("hallo\\s+.*welt.*");
        assertThat(sanitize(result)).matches(pattern);

        result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.translate('Hello world', 'French', '%s')".formatted(modelId)).getOnlyValue();
        pattern = Pattern.compile("bonjour\\s+.*monde.*");
        assertThat(sanitize(result)).matches(pattern);
    }

    public record LabelAndContent(String label, String content) {}

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
