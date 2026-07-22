/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.vertexai;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.genai.Client;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.starburst.ai.model.ConnectionInfo.VertexAiConnectionInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVertexAiClientFactory
{
    private static VertexAiClientFactory factory;

    @BeforeAll
    static void setUp()
    {
        factory = new VertexAiClientFactory(new SecretsResolver(ImmutableMap.of()));
    }

    @Test
    void testCreateClientFromServiceAccountKey()
            throws IOException
    {
        String serviceAccountKey = Resources.toString(getResource("vertex-ai-service-account.json"), StandardCharsets.UTF_8);
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.of(serviceAccountKey),
                Optional.of("my-project"),
                "us-central1",
                ImmutableMap.of());

        try (Client client = factory.createClient(connectionInfo)) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    void testCreateClientDerivesProjectIdFromServiceAccountKey()
            throws IOException
    {
        String serviceAccountKey = Resources.toString(getResource("vertex-ai-service-account.json"), StandardCharsets.UTF_8);
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.of(serviceAccountKey),
                Optional.empty(),
                "us-central1",
                ImmutableMap.of());

        try (Client client = factory.createClient(connectionInfo)) {
            assertThat(client).isNotNull();
        }
    }

    @Test
    void testMissingServiceAccountKeyFailsFast()
    {
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.empty(),
                Optional.of("my-project"),
                "us-central1",
                ImmutableMap.of());

        assertThatThrownBy(() -> factory.createClient(connectionInfo))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Vertex AI requires a serviceAccountKey");
    }

    @Test
    void testCreateClientFailsWhenProjectIdUndecidable()
            throws IOException
    {
        String serviceAccountKeyWithoutProjectId = Resources.toString(getResource("vertex-ai-service-account.json"), StandardCharsets.UTF_8)
                .replace("\"project_id\": \"presto-bq-credentials-test\",\n  ", "");
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.of(serviceAccountKeyWithoutProjectId),
                Optional.empty(),
                "us-central1",
                ImmutableMap.of());

        assertThatThrownBy(() -> factory.createClient(connectionInfo))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Vertex AI projectId is not set and cannot be derived from the service-account key");
    }

    @Test
    void testInvalidServiceAccountKeyFailsFast()
    {
        VertexAiConnectionInfo connectionInfo = new VertexAiConnectionInfo(
                Optional.of("{\"type\":\"service_account\"}"),
                Optional.of("my-project"),
                "us-central1",
                ImmutableMap.of());

        assertThatThrownBy(() -> factory.createClient(connectionInfo))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid Vertex AI service-account key");
    }
}
