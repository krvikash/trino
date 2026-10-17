/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai;

import com.google.common.collect.ImmutableList;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ReasoningEffort;
import io.airlift.json.JsonMapperProvider;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.ai.client.LlmMessage;
import io.starburst.ai.client.StaticPromptDao;
import io.starburst.ai.client.TokenUsageListener;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.starburst.ai.client.MessageRole.USER;
import static org.assertj.core.api.Assertions.assertThat;

class TestOpenAiChatReasoningEffort
{
    private static final ImmutableList<LlmMessage> MESSAGES = ImmutableList.of(new LlmMessage(USER, "hello"));

    @Test
    void testAbsentEffortIsOmitted()
    {
        assertThat(reasoningEffortOf(Optional.empty())).isEmpty();
    }

    @Test
    void testEffortIsPassedThroughUnchanged()
    {
        for (String effort : ImmutableList.of("none", "minimal", "low", "medium", "high", "xhigh", "max")) {
            assertThat(reasoningEffortOf(Optional.of(ReasoningEffort.of(effort)))).contains(ReasoningEffort.of(effort));
        }
    }

    private static Optional<ReasoningEffort> reasoningEffortOf(Optional<ReasoningEffort> configured)
    {
        OpenAiLanguageModelClient client = new OpenAiLanguageModelClient(
                "model",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                new StaticPromptDao(),
                new JsonMapperProvider().get(),
                directExecutor(),
                1,
                OpenAIOkHttpClient.builder().apiKey("unused").build(),
                true,
                configured,
                OpenTelemetry.noop().getTracer("test"),
                "openai",
                false,
                TokenUsageListener.NOOP);
        return client.buildChatCompletionCreateParams(ImmutableList.of(), MESSAGES).build().reasoningEffort();
    }
}
