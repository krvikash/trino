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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.genai.errors.ClientException;
import io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.AiClientErrorCode.INVALID_MODEL_CONFIGURATION;
import static io.starburst.ai.client.TestingUtils.createLlmExecutor;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.high;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.low;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.max;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.medium;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.none;
import static io.starburst.ai.model.LanguageModelConnectionSpec.ReasoningEffort.xhigh;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
public class TestReasoningEffortSupport
{
    private enum Api
    {
        BEDROCK,
        OPENAI_CHAT_COMPLETIONS,
        OPENAI_RESPONSES,
        VERTEX_AI,
    }

    private record Target(String name, Api api, String modelName, Set<ReasoningEffort> accepted, Set<ReasoningEffort> unpinned)
    {
        Target(String name, Api api, String modelName, Set<ReasoningEffort> accepted)
        {
            this(name, api, modelName, accepted, ImmutableSet.of());
        }
    }

    private static final List<Target> TARGETS = ImmutableList.of(
            new Target("sonnet_4_6", Api.BEDROCK, "us.anthropic.claude-sonnet-4-6", ImmutableSet.of(none, low, medium, high, max)),
            new Target("sonnet_5_5", Api.BEDROCK, "global.anthropic.claude-sonnet-5-5", ImmutableSet.of(low, medium, high, xhigh, max)),
            new Target("gpt_6_1_sol_chat", Api.OPENAI_CHAT_COMPLETIONS, "gpt-6.1-sol", ImmutableSet.of(low, medium, high, xhigh)),
            new Target("gpt_6_1_sol_responses", Api.OPENAI_RESPONSES, "gpt-6.1-sol", ImmutableSet.of(low, medium, high, xhigh, max)),
            new Target("gpt_5_6_luna_chat", Api.OPENAI_CHAT_COMPLETIONS, "gpt-5.6-luna", ImmutableSet.of(none, low, medium, high, xhigh)),
            new Target("gpt_5_6_luna_responses", Api.OPENAI_RESPONSES, "gpt-5.6-luna", ImmutableSet.of(none, low, medium, high, xhigh, max)),
            // max is accepted by the API but not in the model documentation, so its result is not pinned
            new Target("gemini_3_8_flash", Api.VERTEX_AI, "gemini-3.8-flash", ImmutableSet.of(low, medium, high), ImmutableSet.of(max)));

    private static final String PROMPT = "Reply with the single word: ok";

    private ScheduledExecutorService reloadingExecutor;
    private ExecutorService llmExecutor;
    private ModelClientProvider modelClientProvider;

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();
        modelClientProvider = staticModelClientProvider(modelSpecs(), reloadingExecutor, llmExecutor);
    }

    @AfterAll
    public void cleanup()
    {
        reloadingExecutor.shutdownNow();
        llmExecutor.shutdownNow();
    }

    // Cases of one provider run one after another to stay under its rate limits; providers run concurrently.
    @Nested
    @Execution(CONCURRENT)
    class Bedrock
    {
        @Execution(SAME_THREAD)
        @ParameterizedTest(name = "{0}")
        @MethodSource("io.starburst.ai.client.TestReasoningEffortSupport#bedrockCases")
        void testReasoningEffort(String specId, Api api, boolean accepted)
        {
            assertReasoningEffort(specId, api, accepted);
        }
    }

    @Nested
    @Execution(CONCURRENT)
    class OpenAi
    {
        @Execution(SAME_THREAD)
        @ParameterizedTest(name = "{0}")
        @MethodSource("io.starburst.ai.client.TestReasoningEffortSupport#openAiCases")
        void testReasoningEffort(String specId, Api api, boolean accepted)
        {
            assertReasoningEffort(specId, api, accepted);
        }
    }

    @Nested
    @Execution(CONCURRENT)
    class Vertex
    {
        @Execution(SAME_THREAD)
        @ParameterizedTest(name = "{0}")
        @MethodSource("io.starburst.ai.client.TestReasoningEffortSupport#vertexCases")
        void testReasoningEffort(String specId, Api api, boolean accepted)
        {
            assertReasoningEffort(specId, api, accepted);
        }
    }

    private void assertReasoningEffort(String specId, Api api, boolean accepted)
    {
        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice(specId));
        if (accepted) {
            assertThat(client.generate(PROMPT, TokenUsageContext.EMPTY)).isNotBlank();
            return;
        }
        assertThatThrownBy(() -> client.generate(PROMPT, TokenUsageContext.EMPTY))
                .satisfies(e -> assertThat(isRejectedAsInvalid(api, e))
                        .describedAs("expected a validation error from the provider but got: %s", Throwables.getRootCause(e))
                        .isTrue());
    }

    public static Stream<Arguments> bedrockCases()
    {
        return cases(Api.BEDROCK);
    }

    public static Stream<Arguments> openAiCases()
    {
        return cases(Api.OPENAI_CHAT_COMPLETIONS, Api.OPENAI_RESPONSES);
    }

    public static Stream<Arguments> vertexCases()
    {
        return cases(Api.VERTEX_AI);
    }

    private static Stream<Arguments> cases(Api... apis)
    {
        Set<Api> selected = ImmutableSet.copyOf(apis);
        return TARGETS.stream()
                .filter(target -> selected.contains(target.api()))
                .flatMap(target -> efforts(target).map(effort -> Arguments.of(
                        specId(target, effort),
                        target.api(),
                        effort.map(target.accepted()::contains).orElse(true))));
    }

    private static boolean isRejectedAsInvalid(Api api, Throwable throwable)
    {
        return switch (api) {
            case BEDROCK, OPENAI_CHAT_COMPLETIONS, OPENAI_RESPONSES -> throwable instanceof TrinoException e
                    && e.getErrorCode().equals(INVALID_MODEL_CONFIGURATION.toErrorCode());
            case VERTEX_AI -> Throwables.getCausalChain(throwable).stream()
                    .anyMatch(cause -> cause instanceof ClientException e && e.code() == 400);
        };
    }

    private static Stream<Optional<ReasoningEffort>> efforts(Target target)
    {
        return Stream.concat(
                Stream.of(Optional.empty()),
                Stream.of(ReasoningEffort.values())
                        .filter(effort -> !target.unpinned().contains(effort))
                        .map(Optional::of));
    }

    private static String specId(Target target, Optional<ReasoningEffort> effort)
    {
        return target.name() + "_" + effort.map(Enum::name).orElse("default");
    }

    private static String modelSpecs()
    {
        String specs = TARGETS.stream()
                .flatMap(target -> efforts(target).map(effort -> modelSpec(target, effort)))
                .collect(joining(",\n"));
        return "{\"models\": [" + specs + "]}";
    }

    private static String modelSpec(Target target, Optional<ReasoningEffort> effort)
    {
        ImmutableList.Builder<String> fields = ImmutableList.builder();
        fields.add("\"id\": \"%s\"".formatted(specId(target, effort)));
        fields.add("\"modelName\": \"%s\"".formatted(target.modelName()));
        fields.add("\"kind\": \"GENERATE\"");
        fields.add("\"connectionInfo\": " + connectionInfo(target.api()));
        effort.ifPresent(value -> fields.add("\"reasoningEffort\": \"%s\"".formatted(value.name())));
        if (target.api() == Api.OPENAI_RESPONSES) {
            fields.add("\"useResponsesApi\": \"true\"");
        }
        return "{" + String.join(", ", fields.build()) + "}";
    }

    private static String connectionInfo(Api api)
    {
        return switch (api) {
            case BEDROCK -> """
                            {
                                "provider": "AWS_BEDROCK",
                                "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                                "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                                "region": "us-east-1"
                            }""";
            case OPENAI_CHAT_COMPLETIONS, OPENAI_RESPONSES -> """
                                                              {
                                                                  "provider": "OPENAI",
                                                                  "endpoint": "https://api.openai.com/v1",
                                                                  "apiKey": "${ENV:OPEN_AI_API_KEY}"
                                                              }""";
            case VERTEX_AI -> """
                              {
                                  "provider": "VERTEX_AI",
                                  "serviceAccountKey": "${ENV:VERTEX_SERVICE_ACCOUNT_KEY}",
                                  "location": "global"
                              }""";
        };
    }
}
