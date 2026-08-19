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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starburstdata.trino.plugin.functions.FunctionsPlugin;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.File;
import java.util.Map;

import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class AiQueryRunner
{
    public static final String STARBURST_FUNCTIONS_CATALOG = "starburst";

    public static final Session TEST_AI_SESSION = testSessionBuilder()
            .setCatalog(STARBURST_FUNCTIONS_CATALOG)
            .build();
    public static final Session TEST_AI_SESSION_BATCH = testSessionBuilder()
            .setCatalog(STARBURST_FUNCTIONS_CATALOG)
            .setCatalogSessionProperty(STARBURST_FUNCTIONS_CATALOG, "batch_calling_enabled", "true")
            .build();

    private AiQueryRunner() {}

    public static void addStarburstAiCatalog(String modelSpecJson, QueryRunner runner)
    {
        requireNonNull(modelSpecJson, "modelSpecJson is null");
        requireNonNull(runner, "runner is null");
        addStarburstAiCatalog(starburstAiFileStorageProperties(modelSpecJson), runner);
    }

    public static void addStarburstAiCatalogWithExternalProvider(QueryRunner runner)
    {
        addStarburstAiCatalog(starburstAiExternalStorageProperties(), runner);
    }

    public static void addStarburstAiCatalog(File modelSpecsFile, QueryRunner runner)
    {
        requireNonNull(modelSpecsFile, "modelSpecsFile is null");
        requireNonNull(runner, "runner is null");
        addStarburstAiCatalog(starburstAiFileStorageProperties(modelSpecsFile), runner);
    }

    private static void addStarburstAiCatalog(Map<String, String> properties, QueryRunner runner)
    {
        runner.installPlugin(new FunctionsPlugin());
        runner.createCatalog(STARBURST_FUNCTIONS_CATALOG, "starburst_functions", properties);
    }

    public static Map<String, String> starburstAiFileStorageProperties(String modelSpecJson)
    {
        requireNonNull(modelSpecJson, "modelSpecJson is null");
        return starburstAiFileStorageProperties(createModelConnectionSpecsFile(modelSpecJson));
    }

    public static Map<String, String> starburstAiFileStorageProperties(File modelSpecsFile)
    {
        requireNonNull(modelSpecsFile, "modelSpecsFile is null");
        return ImmutableMap.of("ai.client.models.storage", "FILE", "ai.client.models.file", modelSpecsFile.getAbsolutePath(), "ai.client.cache.refresh.enabled", "true");
    }

    public static Map<String, String> starburstAiExternalStorageProperties()
    {
        return ImmutableMap.of(
                "ai.client.models.storage", "EXTERNAL",
                "ai.client.cache.refresh.enabled", "true");
    }

    public static Session sessionWithRole(String role)
    {
        return testSessionBuilder()
                .setCatalog(STARBURST_FUNCTIONS_CATALOG)
                .setIdentity(Identity.forUser("user").withEnabledRoles(ImmutableSet.of(role)).build())
                .build();
    }

    static void main()
            throws Exception
    {
        String json =
                """
                {
                    "models": [
                        {
                            "id": "titan_embed_v2",
                            "modelName": "amazon.titan-embed-text-v2:0",
                            "kind": "EMBED",
                            "connectionInfo": {
                                "provider": "AWS_BEDROCK",
                                "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                                "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                                "region": "us-east-1"
                            }
                        },
                        {
                            "id": "meta_llama",
                            "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                            "kind": "GENERATE",
                            "temperature": 0.1,
                            "prompts": {
                                "systemPrompts": [
                                    "You are a useful assistant"
                                ],
                                "analyzeSentimentPrompt": "Analyze the sentiment of the text below. Classify it using one of the following categories: [positive, negative, neutral, mixed]. Use the following labels for the categories: positive = rad, negative = bummer, neutral = meh, neutral = so-so. Output only the label. Do not output anything else.\\n=====%s",
                                "maskPrompt": "Mask the values for each of the JSON encoded labels in the text below. Labels: %s\\nReplace the values with the text \\"[***]\\". Output only the masked text. Do not output anything else.\\n=====%s"
                            },
                            "connectionInfo": {
                                "provider": "AWS_BEDROCK",
                                "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                                "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                                "region": "us-east-1"
                            }
                        },
                        {
                            "id": "embedding3_small",
                            "modelName": "text-embedding-3-small",
                            "kind": "EMBED",
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        },
                        {
                            "id": "gpt4o_mini",
                            "modelName": "gpt-4o-mini",
                            "kind": "GENERATE",
                            "temperature": 0.7,
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        }
                    ]
                }
                """.stripIndent();

        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setAdditionalSetup(runner -> addStarburstAiCatalog(json, runner))
                .build();
        Logger log = Logger.get(AiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
