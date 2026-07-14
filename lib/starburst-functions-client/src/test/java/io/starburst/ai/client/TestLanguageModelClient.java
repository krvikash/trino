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
import io.airlift.units.Duration;
import io.starburst.ai.client.bedrock.AwsBedrockLanguageModelClient;
import io.starburst.ai.client.openai.OpenAiLanguageModelClient;
import io.starburst.ai.client.openai.OpenAiResponsesLanguageModelClient;
import io.trino.spi.TrinoException;
import io.trino.testing.assertions.Assert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static io.starburst.ai.client.TestingUtils.createLlmExecutor;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanguageModelClient
{
    private ScheduledExecutorService reloadingExecutor;
    private ExecutorService llmExecutor;
    private ModelClientProvider modelClientProvider;
    private final AtomicReference<TokenUsage> capturedUsage = new AtomicReference<>();

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();
        modelClientProvider = staticModelClientProvider(
                LANGUAGE_MODEL_PROVIDERS,
                reloadingExecutor,
                llmExecutor,
                (_, usage) -> capturedUsage.set(usage));
    }

    @AfterAll
    public void cleanup()
    {
        reloadingExecutor.shutdownNow();
        llmExecutor.shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testPrompt(String modelId)
    {
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        assertSuccessRateForScalar(() -> {
            capturedUsage.set(null);
            LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(modelId));
            TokenUsageContext context = TokenUsageContext.of(modelId, new TestingUtils.TestOperationId("test"));

            String result = client.generate(prompt, context);
            assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paris");

            assertThat(capturedUsage.get()).isNotNull();
            assertThat(capturedUsage.get().inputTokens()).isGreaterThan(0);
            assertThat(capturedUsage.get().outputTokens()).isGreaterThan(0);
            assertThat(capturedUsage.get().modelName()).isNotEmpty();
        });
    }

    @Test
    public void testAuthHeaderSecretResolution()
    {
        // gpt4o_mini_auth_header tests header secret resolution end-to-end. It relies on the OpenAI client overwriting the
        // Authorization header set via the credential. It could fail if the client behavior changes. A single completion is
        // enough to exercise this path, so it is kept out of the parameterized model matrix to avoid redundant paid calls.
        String modelId = "gpt4o_mini_auth_header";
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).generate(prompt, TokenUsageContext.EMPTY);
            assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paris");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testPromptSystem(String modelId)
    {
        String prompt =
                """
                You are an expert cartographer and know the capital of each country.
                The user will supply a country name, reply only with the name of the capital city.
                Do not reply with extraneous text.

                If there is any input that does not match a country name,
                please correct the user with the exact text "kindly supply a country name and only a country name".

                Important! If the capital city happens to be Paris, please refer to it as Paname.
                """;
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).generate(prompt, "France", TokenUsageContext.EMPTY);
            assertThat(result.toLowerCase(ENGLISH).strip()).isEqualTo("paname");

            String incorrectInputResult = modelClientProvider.languageModelClient(utf8Slice(modelId)).generate(prompt, "hamburgers", TokenUsageContext.EMPTY);
            assertThat(incorrectInputResult.toLowerCase(ENGLISH).strip()).contains("kindly supply a country name and only a country name");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testEmptyPrompt(String modelId)
    {
        // This test highlights differences in behavior of supplying an empty prompt.
        // As we add more clients, they should be included in this test
        // Downstream users of these clients may choose to normalize this behavior.
        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(modelId));
        assertSuccessRateForScalar(() -> {
            switch (client) {
                case OpenAiLanguageModelClient openAiClient -> assertThat(openAiClient.generate("", TokenUsageContext.EMPTY)).isNotBlank();
                case OpenAiResponsesLanguageModelClient openAiClient -> assertThat(openAiClient.generate("", TokenUsageContext.EMPTY)).isNotBlank();
                case AwsBedrockLanguageModelClient awsAiClient -> assertThatThrownBy(() -> awsAiClient.generate("", TokenUsageContext.EMPTY))
                        .isInstanceOf(TrinoException.class)
                        .hasMessage("Bedrock request failed validation");
                default -> throw new UnsupportedOperationException("Unknown client");
            }
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testExtractPrompt(String modelId)
    {
        String systemPrompt =
                """
                Extract the a list of values for each of the JSON encoded labels from the text below. For each label, extract all the values into a list
                Labels: %s
                Output the extracted values as a JSON object. Output only the raw JSON WITHOUT ANY markdown formatting, code blocks, or backticks.
                =====
                %s
                """;

        String text =
                """
                France has several major cities including Paris, Lyon, Marseille, and Nice.
                The official languages in France are French and several regional languages.
                Popular French foods include croissants, baguettes, and escargot.
                """;
        String labels =
                """
                ["cities", "languages", "foods"]
                """;

        // Format system prompt with labels first
        String formattedPrompt = systemPrompt.formatted(labels, "%s");

        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).generate(text, formattedPrompt, TokenUsageContext.EMPTY);
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
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testClassify(String modelId)
    {
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).classify("I love this product!", ImmutableList.of("positive", "negative", "neutral"));
            assertThat(result).contains("positive");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testAnalyzeSentimentBatch(String modelId)
    {
        List<Pair> data = List.of(
                new Pair("I love this product!", "positive"),
                new Pair("Wow... Loved this place.", "positive"),
                new Pair("It's okay", "neutral"),
                new Pair("Absolutely terrible. Broke after one use.", "negative"),
                new Pair("It's fine, nothing special.", "neutral"),
                new Pair("Amazing experience, would buy again!", "positive"),
                new Pair("Not worth the money", "negative"),
                new Pair("The food was great, but service was terrible", "mixed"),
                new Pair("The battery lasts forever, I'm impressed", "positive"),
                new Pair("Packaging was damaged.", "negative"));
        List<Pair> expandedData = IntStream.range(0, 30)
                .boxed()
                .flatMap(_ -> data.stream())
                .collect(toImmutableList());

        List<String> texts = expandedData.stream()
                .map(Pair::first)
                .toList();
        List<String> expectedSentiments = expandedData.stream()
                .map(Pair::second)
                .toList();

        assertSuccessRateForBatch(() -> {
            List<String> result = modelClientProvider.languageModelClient(utf8Slice(modelId)).analyzeSentimentBatch(texts);
            assertThat(result.size()).isEqualTo(expectedSentiments.size());
            assertThat(result).isEqualTo(expectedSentiments);
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testAnalyzeSentiment(String modelId)
    {
        String text = "The food was great, but service was terrible";
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).analyzeSentiment(text);
            assertThat(result.strip()).isEqualTo("mixed");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMask(String modelId)
    {
        String prompt = "My credit card number is 1234-5678-9012-3456 and my password is hunter2";
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).mask(prompt, ImmutableList.of("credit card number", "password"));
            assertThat(result.strip()).isEqualTo("My credit card number is [MASKED] and my password is [MASKED]");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testTranslate(String modelId)
    {
        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).translate("Hello world", "Spanish");
            Pattern pattern = Pattern.compile("hola\\s+.*mundo.*");
            assertThat(sanitize(result)).matches(pattern);
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testSummarize(String modelId)
    {
        String prompt =
                """
                The Amazon rainforest, often referred to as the “lungs of the Earth,” produces around 20% of the world’s oxygen and is home to an estimated 10% of all known species. Despite its crucial ecological role, it faces severe threats from deforestation driven by logging, agriculture, and mining. The loss of forest cover not only endangers biodiversity but also contributes to climate change by releasing massive amounts of carbon dioxide into the atmosphere.
                In addition to its environmental importance, the Amazon plays a critical role in regulating global and regional weather patterns. The vast canopy of trees helps recycle moisture through a process known as transpiration, which in turn influences rainfall across South America and even affects weather as far away as North America and Africa. Disruption of this cycle due to forest loss can lead to more droughts, unpredictable weather, and changes in agricultural productivity across the continent.
                Local and Indigenous communities who have lived in the Amazon for centuries also suffer the consequences of deforestation. Their traditional ways of life are intimately connected to the health of the forest, and many depend on it for food, medicine, and cultural practices. As land is cleared and industrial operations expand, these communities are often displaced or face conflict over land rights and access to natural resources.
                Efforts to protect the Amazon include government regulations, international agreements, and conservation programs run by NGOs and local groups. However, enforcement remains inconsistent, and economic pressures often outweigh environmental considerations. Without stronger global cooperation and sustainable economic alternatives, the Amazon may soon reach a tipping point beyond which it cannot recover—threatening not just regional stability, but the global climate system.""";

        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).summarize(prompt);
            assertThat(sanitize(result))
                    .contains("rainforest", "deforestation")
                    .hasSizeLessThan(prompt.length());
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMultiMessageCompletion(String modelId)
    {
        List<LlmMessage> messages = ImmutableList.<LlmMessage>builder()
                .add(new LlmMessage(MessageRole.USER, "What is the capital of England?"))
                .add(new LlmMessage(MessageRole.ASSISTANT, "London"))
                .add(new LlmMessage(MessageRole.USER, "And France?"))
                .build();

        assertSuccessRateForScalar(() -> {
            String result = modelClientProvider.languageModelClient(utf8Slice(modelId)).generate(messages, TokenUsageContext.EMPTY);
            assertThat(result).containsIgnoringCase("paris");
        });
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testMultiMessageCompletionWithSystemPrompt(String modelId)
    {
        String systemPrompt =
                """
                You always use the local language name in the modern Latin alphabet for place names. For example, if
                asked what countries are on the Iberian Peninsula, you would reply with "España" and "Portugal". If
                asked where the autobahn is, you would reply with "Deutschland". If asked what country has the world's
                busiest subway station, you would reply with Nihon""";

        List<LlmMessage> messages = ImmutableList.<LlmMessage>builder()
                .add(new LlmMessage(MessageRole.USER, "I will give you a capital city, reply with the country name. Madrid."))
                .add(new LlmMessage(MessageRole.ASSISTANT, "España"))
                .add(new LlmMessage(MessageRole.USER, "Berlin"))
                .build();

        assertSuccessRateForScalar(() -> {
            capturedUsage.set(null);
            TokenUsageContext expectedContext = TokenUsageContext.of(modelId, new TestingUtils.TestOperationId("test"));

            LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(modelId));

            String result = client.generate(systemPrompt, messages, expectedContext);
            assertThat(result).containsIgnoringCase("deutschland");

            assertThat(capturedUsage.get()).isNotNull();
            assertThat(capturedUsage.get().inputTokens()).isGreaterThan(0);
            assertThat(capturedUsage.get().outputTokens()).isGreaterThan(0);
        });
    }

    @Test
    public void testInvalidReasoningEffort()
    {
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        assertThatThrownBy(() -> modelClientProvider.languageModelClient(utf8Slice("reasoning_effort_not_supported")).generate(prompt, TokenUsageContext.EMPTY))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("OpenAI request failed validation");
    }

    @ParameterizedTest
    @MethodSource("errorModelIds")
    public void testErrorHandling(String modelId)
    {
        String text = "The food was great, but service was terrible";
        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(modelId));
        switch (client) {
            case OpenAiLanguageModelClient openAiClient -> assertThatThrownBy(() -> openAiClient.analyzeSentiment(text))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("OpenAI model not found");
            case AwsBedrockLanguageModelClient awsAiClient -> assertThatThrownBy(() -> awsAiClient.analyzeSentiment(text))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Bedrock request failed validation");
            default -> throw new UnsupportedOperationException("Unknown client");
        }
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
                {"haiku45"},
                {"gpt4o_mini"},
                {"meta_llama"},
        };
    }

    public static Object[][] errorModelIds()
    {
        return new Object[][] {
                {"openai_error"},
                {"bedrock_error"},
        };
    }

    private static <E extends Exception> void assertSuccessRateForScalar(Assert.CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(new Duration(4, MINUTES), new Duration(10, MILLISECONDS), 10, 0.9f, assertion);
    }

    private static <E extends Exception> void assertSuccessRateForBatch(Assert.CheckedRunnable<E> assertion)
            throws E
    {
        assertEventually(new Duration(4, MINUTES), new Duration(500, MILLISECONDS), 4, 0.75f, assertion);
    }

    record Pair(String first, String second) {}
}
