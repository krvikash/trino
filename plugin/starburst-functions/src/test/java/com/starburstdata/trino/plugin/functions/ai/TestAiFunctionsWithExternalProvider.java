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

import com.google.inject.Key;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiFileStorageConfig;
import io.starburst.ai.client.FileBackedModelConnectionSpecsLoader;
import io.starburst.ai.model.ModelConnectionSpecsLoader;
import io.trino.server.InternalHttpClient;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.server.ai.RemoteModelConnectionSpecsLoader.BASE_PATH;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
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
                            "awsAccessKey": "${ENV:AWS_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:AWS_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    }
                ]
            }""".formatted(MODEL_ID);

    @Test
    public void testClassify()
    {
        Request request = prepareGet()
                .setUri(getQueryRunner().getCoordinator().resolve(BASE_PATH + "/caller-count"))
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        HttpClient httpClient = getQueryRunner().getCoordinator().getInstance(Key.get(HttpClient.class, InternalHttpClient.class));
        // wait until the model providers on all workers had a chance to load the models
        assertEventually(
                new Duration(20, SECONDS),
                () -> assertThat(Integer.parseInt(httpClient.execute(request, createStringResponseHandler()).getBody()))
                        .isEqualTo(getQueryRunner().getNodeCount() - 1));
        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(MODEL_ID)).getOnlyValue();
        assertThat(result).contains("positive");
    }
}
