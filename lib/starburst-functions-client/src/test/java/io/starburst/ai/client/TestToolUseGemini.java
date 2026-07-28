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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.VendorTestModels.GEMINI_MODEL_PROVIDERS;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID_NON_STREAMING;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestToolUseGemini
        extends BaseTestToolUse
{
    @Test
    public void testStreamingNonStreamingLLMsThrows()
    {
        CalculatorTool tool = new CalculatorTool();

        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, Optional.of("What is 25 + 37? Use the calculator tool."), ImmutableList.of(), ImmutableList.of()));

        assertThatThrownBy(() -> executeToolUse(
                LANGUAGE_MODEL_ID,
                "You are a helpful assistant with access to a calculator.",
                messages,
                ImmutableList.of(tool))).isInstanceOf(TrinoException.class);
    }

    @Override
    protected String getLanguageModelProviders()
    {
        return GEMINI_MODEL_PROVIDERS;
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {{LANGUAGE_MODEL_ID_NON_STREAMING}};
    }
}
