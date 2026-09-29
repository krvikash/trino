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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.VendorTestModels.AZURE_OPEN_AI_MODEL_PROVIDERS;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID_NON_STREAMING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestToolUseAzureOpenAi
        extends BaseTestToolUse
{
    // Non-deterministic: Azure OpenAI sends content filter annotation chunks after finish_reason only for some responses
    @Test
    public void testStreamingWithContentFilterAnnotationsAfterFinishReason()
    {
        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("What is 25 + 37? Answer with just the number."), ImmutableList.of(), ImmutableList.of()));

        ToolUseResponse response = executeToolUse(
                LANGUAGE_MODEL_ID,
                "You are a helpful assistant",
                messages,
                ImmutableList.of());

        assertThat(response.textResponse()).contains("62");
    }

    @Override
    protected String getLanguageModelProviders()
    {
        return AZURE_OPEN_AI_MODEL_PROVIDERS;
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {
                {LANGUAGE_MODEL_ID},
                {LANGUAGE_MODEL_ID_NON_STREAMING},
        };
    }
}
