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

import io.trino.testing.services.junit.Flaky;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static io.starburst.ai.client.VendorTestModels.AZURE_OPEN_AI_RESPONSES_API_MODEL_PROVIDERS;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID_NON_STREAMING;

public class TestToolUseAzureOpenAiResponsesApi
        extends BaseTestToolUse
{
    @Override
    protected String getLanguageModelProviders()
    {
        return AZURE_OPEN_AI_RESPONSES_API_MODEL_PROVIDERS;
    }

    @Override
    protected boolean usesOAuth2()
    {
        return true;
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {
                {LANGUAGE_MODEL_ID},
                {LANGUAGE_MODEL_ID_NON_STREAMING},
        };
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    @Override
    @Flaky(issue = "https://starburstdata.atlassian.net/browse/ENG-29405", match = "StreamResetException: stream was reset: INTERNAL_ERROR")
    public void testToolCallWithNoParameters(String modelId)
    {
        super.testToolCallWithNoParameters(modelId);
    }
}
