/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import io.airlift.json.JsonCodec;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.STARBURST_FUNCTIONS_CATALOG;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION_BATCH;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.addStarburstAiCatalog;
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
                .amendSession(sessionBuilder ->
                        sessionBuilder.setCatalogSessionProperty(STARBURST_FUNCTIONS_CATALOG, "batch_calling_enabled", "true"))
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
                "SELECT ai.classify('Rice', ARRAY['fruit', 'vegetable', 'grain', 'meat', 'dairy', 'nuts', 'something else'], '%s')".formatted(modelId)).getOnlyValue();
        assertThat(result).contains("grain");
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

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testSummarize(String modelId)
    {
        String prompt = """
               The Amazon rainforest, often referred to as the “lungs of the Earth,” produces around 20% of the world’s oxygen and is home to an estimated 10% of all known species. Despite its crucial ecological role, it faces severe threats from deforestation driven by logging, agriculture, and mining. The loss of forest cover not only endangers biodiversity but also contributes to climate change by releasing massive amounts of carbon dioxide into the atmosphere.
               In addition to its environmental importance, the Amazon plays a critical role in regulating global and regional weather patterns. The vast canopy of trees helps recycle moisture through a process known as transpiration, which in turn influences rainfall across South America and even affects weather as far away as North America and Africa. Disruption of this cycle due to forest loss can lead to more droughts, unpredictable weather, and changes in agricultural productivity across the continent.
               Local and Indigenous communities who have lived in the Amazon for centuries also suffer the consequences of deforestation. Their traditional ways of life are intimately connected to the health of the forest, and many depend on it for food, medicine, and cultural practices. As land is cleared and industrial operations expand, these communities are often displaced or face conflict over land rights and access to natural resources.
               Efforts to protect the Amazon include government regulations, international agreements, and conservation programs run by NGOs and local groups. However, enforcement remains inconsistent, and economic pressures often outweigh environmental considerations. Without stronger global cooperation and sustainable economic alternatives, the Amazon may soon reach a tipping point beyond which it cannot recover—threatening not just regional stability, but the global climate system.""";

        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.summarize('%s', '%s')".formatted(prompt, modelId)).getOnlyValue();
        assertThat(sanitize(result))
                .contains("rainforest", "deforestation")
                .hasSizeLessThan(prompt.length());
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testAnalyzeSentimentBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id) AS (SELECT random() FROM UNNEST(SEQUENCE(1, 200))),
                        r(review, label) AS (
                            VALUES
                                ('The product is amazing' || CHR(10) || 'The best thing since sliced bread!', 'positive'),
                                ('I hate this product passionately!', 'negative'),
                                ('', null),
                                ('I like the taste but it smells bad', 'mixed'),
                                (null, null),
                                ('I could take it or leave it', 'neutral')
                        ),
                        randomized AS (
                            SELECT review, label
                            from r cross join seq
                            -- randomize the order to avoid predictable patterns for the LLM
                            order by id * if(review is null or review = '', 25, length(review))
                            -- ordering is ignored without a limit
                            limit 1200
                        ),
                        t AS (
                            SELECT review, starburst.ai.analyze_sentiment(review, '%s') llm_response, label
                            FROM randomized
                        )
                        SELECT *
                        FROM t
                        where llm_response is distinct from label
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testClassifyBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id) AS (SELECT random() FROM UNNEST(SEQUENCE(1, 50))),
                        r(review, label) AS (
                            VALUES
                                ('Apple', 'fruit'),
                                ('Broccoli', 'vegetable'),
                                ('Rice', 'grain'),
                                ('Chicken', 'meat'),
                                ('Cheddar cheese', 'dairy'),
                                ('Almonds', 'nuts'),
                                ('Chocolate', 'something else'),
                                ('Banana', 'fruit'),
                                ('Carrot', 'vegetable'),
                                (null, null),
                                ('Steak', 'meat'),
                                ('Yogurt', 'dairy')
                        ),
                        randomized AS (
                            SELECT review, label
                            from r cross join seq
                            -- randomize the order to avoid predictable patterns for the LLM
                            order by id * if(review is null or review = '', 25, length(review))
                            -- ordering is ignored without a limit
                            limit 1200
                        ),
                        t AS (
                            SELECT review, starburst.ai.classify(review, ARRAY['fruit', 'vegetable', 'grain', 'meat', 'dairy', 'nuts', 'something else'], '%s') llm_response, label
                            FROM randomized
                        )
                        SELECT *
                        FROM t
                        where llm_response is distinct from label
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testFixGrammarBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id, seq_no) AS (SELECT floor(6 * random()), seq_no FROM UNNEST(sequence(1, 300)) AS t(seq_no)),
                        r(id, incorrect, corrected) AS (
                            VALUES
                                (0, 'she past the test wit ease.', 'She passed the test with ease.'),
                                (1, 'the weather will effect our plans.', 'The weather will affect our plans.'),
                                (2, '', null),
                                (3, 'The principle gave a long speech.' || CHR(10) || 'It’s conclusion? Spelling is an important skill.', 'The principal gave a long speech.' || CHR(10) || 'Its conclusion? Spelling is an important skill.'),
                                (4, null, null),
                                (5, 'She was more happier in canada.', 'She was happier in Canada.')
                        ),
                        t AS (
                            SELECT incorrect, starburst.ai.fix_grammar(incorrect, '%s') llm_response, corrected
                            FROM r JOIN seq USING (id)
                        )
                        SELECT *
                        FROM t
                        WHERE llm_response IS DISTINCT FROM corrected
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMaskBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id, seq_no) AS (SELECT floor(6 * random()), seq_no FROM UNNEST(sequence(1, 300)) AS t(seq_no)),
                        r(id, unmasked, masked) AS (
                            VALUES
                                (0, 'Text me at 202-555-1234.', 'Text me at [MASKED].'),
                                (1, 'I charged the meal on card 1234-0987-4567-3456.', 'I charged the meal on card [MASKED].'),
                                (2, '', ''),
                                (3, 'I reset your password to T9v#eLp3!zQa and sent it to bob@acme.com.' || CHR(10) || 'Change it next time you log in.', 'I reset your password to [MASKED] and sent it to [MASKED].' || CHR(10) || 'Change it next time you log in.'),
                                (4, null, null),
                                (5, 'She was checking her email every minute.', 'She was checking her email every minute.')
                        ),
                        t AS (
                            SELECT unmasked, starburst.ai.mask(unmasked, ARRAY['phone number', 'credit card number', 'email', 'password'], '%s') llm_response, masked
                            FROM r JOIN seq USING (id)
                        )
                        SELECT *
                        FROM t
                        WHERE llm_response IS DISTINCT FROM masked
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testTranslateBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id, seq_no) AS (SELECT floor(6 * random()), seq_no FROM UNNEST(sequence(1, 300)) AS t(seq_no)),
                        r(id, english) AS (
                            VALUES
                                (0, 'I am learning French, but it is still difficult for me.'),
                                (1, 'I’m going to the store.' || CHR(10) || 'Do you need anything?'),
                                (2, ''),
                                (3, 'I forgot my keys at home.'),
                                (4, null),
                                (5, 'She doesn’t like coffee, but she loves tea.')
                        ),
                        t AS (
                            SELECT english, starburst.ai.translate(english, 'french', '%1$s') llm_response
                            FROM r JOIN seq USING (id)
                        ),
                        llm_verification AS (
                            SELECT english, llm_response,
                                   starburst.ai.prompt('You are an expert English-French translator. Is the following a reasonable translation of the English sentence? Only reply with yes or no. Do not provide explanations.' ||
                                   CHR(10) || 'English: ' || english ||
                                   CHR(10) || 'French: ' || llm_response, '%1$s') as is_good_translation
                            FROM t
                            WHERE (english IS NOT null) AND (english != '')
                            GROUP BY english, llm_response
                            UNION
                            SELECT english, llm_response, if (llm_response = '', 'yes', 'no') as is_good_translation
                            FROM t
                            WHERE english = ''
                            UNION
                            SELECT english, llm_response, if (llm_response IS null, 'yes', 'no') as is_good_translation
                            FROM t
                            WHERE english IS null
                        )
                        SELECT *
                        FROM llm_verification
                        WHERE lower(is_good_translation) NOT LIKE '%%yes%%'
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testSummarizeBatch(String modelId)
    {
        List<MaterializedRow> result = computeActual(TEST_AI_SESSION_BATCH,
                """
                        WITH
                        seq(id, seq_no) AS (SELECT floor(6 * random()), seq_no FROM UNNEST(sequence(1, 300)) AS t(seq_no)),
                        r(id, paragraph) AS (
                            VALUES
                                (0, 'Julia had always dreamed of opening her own bakery. After years of working in restaurants and saving every penny, she finally found a small shop for rent in her neighborhood. She spent months renovating the space, testing recipes, and building a small team to help run the place.'),
                                (1, 'Climate change is causing noticeable shifts in global weather patterns. Many regions are experiencing more frequent and intense heatwaves, while others face heavier rainfall and flooding. These changes are not only affecting ecosystems but also human health, agriculture, and infrastructure.'),
                                (2, ''),
                                (3, 'A recent study on Type 2 diabetes revealed that early lifestyle interventions can significantly reduce the need for medication. Patients who followed a structured program of diet changes, increased physical activity, and regular check-ins with health professionals showed notable improvements in blood sugar levels.'),
                                (4, null),
                                (5, 'After a period of rapid growth, the startup TechNova faced several operational challenges. Departments were siloed, communication slowed, and projects started missing deadlines. Employees reported feeling overwhelmed by shifting priorities and unclear goals.' || CHR(10) ||
                                    'To address these issues, the leadership team implemented new project management tools, established regular cross-department meetings, and clarified company objectives. Over time, these changes helped improve collaboration and efficiency across the organization.')
                        ),
                        t AS (
                            SELECT paragraph, starburst.ai.summarize(paragraph, '%1$s') llm_response
                            FROM r JOIN seq USING (id)
                        ),
                        llm_verification AS (
                            SELECT paragraph, llm_response,
                                   starburst.ai.prompt('You are a summarization AI assistant. Assess whether the following text is reasonably summarized by the provided summary. Only reply with yes or no. Do not provide explanations.' ||
                                   CHR(10) || 'Text: ' || paragraph ||
                                   CHR(10) || 'Summary: ' || llm_response, '%1$s') as is_good_summary
                            FROM t
                            GROUP BY paragraph, llm_response
                        )
                        SELECT *
                        FROM llm_verification
                        WHERE ((paragraph IS NOT null) AND (paragraph != '')) AND (lower(is_good_summary) NOT LIKE '%%yes%%')
                        """.formatted(modelId))
                .getMaterializedRows();
        assertThat(result).isEmpty();
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
