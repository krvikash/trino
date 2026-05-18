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

import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.ModelClientProvider;
import io.starburst.ai.client.TestingUtils;
import io.starburst.ai.client.TokenUsageContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static io.starburst.ai.client.TestingUtils.createLlmExecutor;
import static io.starburst.ai.client.TestingUtils.staticModelClientProvider;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAwsBedrockGptOssResponse
{
    private ModelClientProvider modelClientProvider;
    private ScheduledExecutorService reloadingExecutor;
    private ExecutorService llmExecutor;

    @BeforeAll
    public void setup()
            throws IOException
    {
        reloadingExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("reloading-model-client-provider"));
        llmExecutor = createLlmExecutor();
        modelClientProvider = staticModelClientProvider(LANGUAGE_MODEL_PROVIDERS, reloadingExecutor, llmExecutor);
    }

    @AfterAll
    public void teardown()
    {
        reloadingExecutor.shutdownNow();
        llmExecutor.shutdownNow();
    }

    @Test
    public void testResponseWithReasoningBlocksIsHandled()
    {
        // OpenAI models return a reasoning ContentBlock (no text) followed by the actual response ContentBlock.
        // Make sure we handle that kind of response.
        LanguageModelClient client = modelClientProvider.languageModelClient(utf8Slice("gpt_oss_120b"));
        TokenUsageContext context = TokenUsageContext.of("gpt_oss_120b", new TestingUtils.TestOperationId("test-gpt-oss-reasoning"));

        String response = client.generate("What does NTLM mean in Connectivity?", context);
        assertThat(response).isNotEmpty();
    }
}
