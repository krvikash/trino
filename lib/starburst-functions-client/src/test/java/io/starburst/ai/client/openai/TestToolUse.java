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

import io.starburst.ai.client.BaseTestToolUse;

import static io.starburst.ai.client.TestingUtils.LANGUAGE_MODEL_PROVIDERS;

public class TestToolUse
        extends BaseTestToolUse
{
    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {
                {"gpt4o_mini"},
                {"haiku35"}};
    }

    @Override
    protected String getLanguageModelProviders()
    {
        return LANGUAGE_MODEL_PROVIDERS;
    }
}
