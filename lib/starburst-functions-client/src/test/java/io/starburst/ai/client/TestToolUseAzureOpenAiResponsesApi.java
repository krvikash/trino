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
}
