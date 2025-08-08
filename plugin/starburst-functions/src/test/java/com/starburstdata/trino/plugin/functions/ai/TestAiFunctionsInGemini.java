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

class TestAiFunctionsInGemini
        extends BaseAiFunctionsSmokeTest
{
    private static final String GEMINI_MODEL_PROVIDERS = """
                {
                  "models": [
                     {
                        "id": "%s",
                        "modelName": "gemini-2.0-flash",
                        "kind": "GENERATE",
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://generativelanguage.googleapis.com/v1beta/openai/",
                            "apiKey": "${ENV:GEMINI_API_KEY}"
                        }
                    },
                    {
                      "id": "%s",
                      "modelName": "gemini-embedding-001",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "https://generativelanguage.googleapis.com/v1beta/openai/",
                        "apiKey": "${ENV:GEMINI_API_KEY}"
                      }
                    }
                  ]
                }""".formatted(LANGUAGE_MODEL_ID, EMBED_MODEL_ID);

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(GEMINI_MODEL_PROVIDERS, runner))
                .build();
    }
}
