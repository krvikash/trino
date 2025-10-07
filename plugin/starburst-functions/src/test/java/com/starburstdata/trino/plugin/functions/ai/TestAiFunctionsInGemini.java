/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.QueryRunner;

import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.addStarburstAiCatalog;
import static io.starburst.ai.client.VendorTestModels.GEMINI_MODEL_PROVIDERS;

class TestAiFunctionsInGemini
        extends BaseAiFunctionsSmokeTest
{
    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(GEMINI_MODEL_PROVIDERS, runner))
                .build();
    }
}
