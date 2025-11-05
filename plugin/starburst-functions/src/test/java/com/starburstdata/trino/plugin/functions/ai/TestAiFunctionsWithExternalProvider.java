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
import io.airlift.units.Duration;
import io.starburst.ai.client.AiFileStorageConfig;
import io.starburst.ai.client.FileBackedModelConnectionSpecsLoader;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAiFunctionsWithExternalProvider
        extends AbstractTestQueryFramework
{
    private static final String MODEL_ID = "meta_llama";

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        ModelConnectionSpecsLoader modelConnectionSpecsLoader = new FileBackedModelConnectionSpecsLoader(
                new AiFileStorageConfig()
                        .setModelConnectionSpecsFile(createModelConnectionSpecsFile(LANGUAGE_MODEL_PROVIDERS).getAbsolutePath()));

        return DistributedQueryRunner.builder(testSessionBuilder().build())
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setAdditionalSetup(AiQueryRunner::addStarburstAiCatalogWithExternalProvider)
                .withModelConnectionSpecsLoader(Optional.of(modelConnectionSpecsLoader))
                .build();
    }

    private static final String LANGUAGE_MODEL_PROVIDERS = """
            {
                "models": [
                    {
                        "id": "%s",
                        "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    }
                ]
            }""".formatted(MODEL_ID);

    @Test
    public void testClassify()
    {
        assertEventually(new Duration(30, SECONDS), new Duration(10, MILLISECONDS), 4, 0.75f, () ->
        {
            String result = (String) computeActual(TEST_AI_SESSION,
                    "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(MODEL_ID)).getOnlyValue();
            assertThat(result).contains("positive");
        });
    }
}
