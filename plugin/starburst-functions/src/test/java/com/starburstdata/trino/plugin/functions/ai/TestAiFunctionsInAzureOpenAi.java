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
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

class TestAiFunctionsInAzureOpenAi
        extends BaseAiFunctionsSmokeTest
{
    private static final String AZURE_OPEN_AI_LANGUAGE_ENDPOINT = requireEnv("AZURE_OPEN_AI_LANGUAGE_ENDPOINT");
    private static final String AZURE_OPEN_AI_EMBED_ENDPOINT = requireEnv("AZURE_OPEN_AI_EMBED_ENDPOINT");

    private static final String AZURE_OPEN_AI_MODEL_PROVIDERS = """
                {
                  "models": [
                     {
                        "id": "%s",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "%s",
                            "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                        }
                    },
                    {
                      "id": "%s",
                      "modelName": "text-embedding-3-small",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "%s",
                        "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                      }
                    }
                  ]
                }""".formatted(LANGUAGE_MODEL_ID, AZURE_OPEN_AI_LANGUAGE_ENDPOINT, EMBED_MODEL_ID, AZURE_OPEN_AI_EMBED_ENDPOINT);

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(AZURE_OPEN_AI_MODEL_PROVIDERS, runner))
                .build();
    }
}
