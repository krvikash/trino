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

import static io.starburst.ai.client.VendorTestModels.GEMINI_MODEL_PROVIDERS;
import static io.starburst.ai.client.VendorTestModels.LANGUAGE_MODEL_ID;

public class TestToolUseGemini
        extends BaseTestToolUse
{
    @Override
    protected String getLanguageModelProviders()
    {
        return GEMINI_MODEL_PROVIDERS;
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {{LANGUAGE_MODEL_ID}};
    }
}
