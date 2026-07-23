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

import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static io.starburst.ai.client.VendorTestModels.VERTEX_MODEL_PROVIDERS;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestToolUseVertexAi
        extends BaseTestToolUse
{
    @Override
    protected String getLanguageModelProviders()
    {
        return VERTEX_MODEL_PROVIDERS;
    }

    @Override
    public Object[][] modelIds()
    {
        return new Object[][] {{"vertex_gemini"}};
    }

    @Test
    public void testProjectIdDerivedFromServiceAccountKey()
            throws IOException
    {
        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(
                new ByteArrayInputStream(requireEnv("VERTEX_SERVICE_ACCOUNT_KEY").getBytes(UTF_8)));
        assertThat(credentials.getProjectId()).isEqualTo(requireEnv("VERTEX_PROJECT_ID"));
    }
}
