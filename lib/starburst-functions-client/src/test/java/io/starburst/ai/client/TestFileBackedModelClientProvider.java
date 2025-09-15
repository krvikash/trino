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

import com.google.common.collect.ImmutableSet;
import dev.failsafe.RetryPolicy;
import io.starburst.ai.model.LanguageModelConnectionSpec;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static dev.failsafe.Failsafe.with;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static io.starburst.ai.client.TestingUtils.reloadingModelClientProvider;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFileBackedModelClientProvider
{
    private static final String TEST_MODELS_CONFIG = """
            {
                "models": [
                    {
                        "id": "test-gpt",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 4096,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1"
                        }
                    },
                    {
                        "id": "test-bedrock",
                        "modelName": "us.anthropic.claude-3-haiku-20240307-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 4096,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "region": "us-east-1"
                        }
                    }
                ]
            }
            """;

    private static final String TEST_MODELS_CONFIG_V2 = """
            {
                "models": [
                    {
                        "id": "test-gpt3",
                        "modelName": "gpt-o3",
                        "kind": "GENERATE",
                        "maxTokens": 4096,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1"
                        }
                    },
                    {
                        "id": "test-bedrock",
                        "modelName": "us.anthropic.claude-3-haiku-20240307-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "region": "us-east-1"
                        }
                    }
                ]
            }
            """;

    @Test
    public void testRefreshingModelClientProvider()
            throws IOException
    {
        // Note: there will be warnings in the logs about client creation failures since this is a dummy config.
        // This is not an error condition, this test is to verify the reloading behavior.
        File modelSpecsFile = createModelConnectionSpecsFile(TEST_MODELS_CONFIG);
        ReloadingModelClientProvider modelClientProvider = reloadingModelClientProvider(modelSpecsFile);
        try {
            Collection<LanguageModelConnectionSpec> originalSpecs = modelClientProvider.languageModelConnectionSpecs();
            assertThat(originalSpecs.size()).isEqualTo(2);
            assertThat(originalSpecs.stream()
                    .map(LanguageModelConnectionSpec::id)
                    .collect(toImmutableSet())).containsAll(ImmutableSet.of("test-gpt", "test-bedrock"));
            assertThat(originalSpecs.stream()
                    .filter(spec -> spec.id().equals("test-bedrock"))
                    .findFirst()
                    .get()
                    .maxTokens())
                    .isEqualTo(Optional.of(4096));
            Files.write(modelSpecsFile.toPath(), TEST_MODELS_CONFIG_V2.getBytes(StandardCharsets.UTF_8));
            with(RetryPolicy.builder()
                    .withMaxDuration(Duration.ofSeconds(5))
                    .withDelay(Duration.ofSeconds(1))
                    .build())
                    .run(() -> {
                        Collection<LanguageModelConnectionSpec> newSpecs = modelClientProvider.languageModelConnectionSpecs();
                        assertThat(newSpecs.size()).isEqualTo(2);
                        assertThat(newSpecs.stream()
                                .map(LanguageModelConnectionSpec::id)
                                .collect(toImmutableSet()))
                                .containsAll(ImmutableSet.of("test-gpt3", "test-bedrock"));
                        // Ensure updates to models work as expected
                        assertThat(newSpecs.stream()
                                .filter(spec -> spec.id().equals("test-bedrock"))
                                .findFirst()
                                .get()
                                .maxTokens())
                                .isEqualTo(Optional.of(8192));
                    });
        }
        finally {
            modelClientProvider.shutdown();
        }
    }
}
