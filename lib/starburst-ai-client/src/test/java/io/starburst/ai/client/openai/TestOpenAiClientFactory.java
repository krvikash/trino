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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.starburst.ai.client.openai.OpenAiClientFactory.tryExtractAzureOpenAiConnectionInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpenAiClientFactory
{
    @Test
    void testAzureOpenAiUrlFormat()
    {
        assertThat(tryExtractAzureOpenAiConnectionInfo(Optional.of("https://api.openai.com/v1")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/o4-mini-deployment/chat/completions?api-version=2025-01-01-preview")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com",
                                "o4-mini-deployment",
                                "2025-01-01-preview",
                                false)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/text-embedding-3-small-deployment/embeddings?api-version=2023-05-15")))
                .isEqualTo(Optional.of(
                        new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                                "https://test-domain.openai.azure.com",
                                "text-embedding-3-small-deployment",
                                "2023-05-15",
                                false)));
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/v2/gpt-4o-mini-2023-07-18/chat/completions?api-version=2023-12-01-preview")))
                .isEmpty();
        assertThat(tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test.domain.com/gpt/libsupport/openai/deployments/gpt-4o-2024-11-20/chat/completions?api-version=2024-12-01-preview")))
                .isEqualTo(Optional.of(new OpenAiClientFactory.AzureOpenAiConnectionInfo(
                        "https://test.domain.com/gpt/libsupport/openai/deployments/gpt-4o-2024-11-20",
                        "",
                        "2024-12-01-preview",
                        true)));
        assertThatThrownBy(() -> tryExtractAzureOpenAiConnectionInfo(
                Optional.of("https://test-domain.openai.azure.com/openai/deployments/")))
                .hasMessageContaining("Invalid Azure OpenAI endpoint - missing deployment");
    }
}
