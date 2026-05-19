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
import io.starburst.ai.client.BaseTestToolUse;
import io.starburst.ai.client.LlmMessage;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestToolUse
        extends BaseTestToolUse
{
    @Test
    public void testStreamingNonStreamingLLMsThrows()
    {
        CalculatorTool tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "What is 25 + 37? Use the calculator tool."));

        assertThatThrownBy(() -> executeToolUse(
                "mistral_large",
                "You are a helpful assistant with access to a calculator.",
                messages,
                ImmutableList.of(tool))).isInstanceOf(TrinoException.class);
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {
                {"gpt4o_mini"},
                {"haiku35"},
                {"mistral_large_non_streaming"},
        };
    }

    @Override
    protected String getLanguageModelProviders()
    {
        return LANGUAGE_MODEL_PROVIDERS;
    }
}
