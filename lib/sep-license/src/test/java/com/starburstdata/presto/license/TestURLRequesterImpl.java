/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestURLRequesterImpl
{
    private static final String METADATA_SERVICE_BASE_URL = "http://169.254.169.254";

    @Test
    public void testReadFromResourceURL()
            throws IOException
    {
        URL url = getResource(getClass(), "test-aws_identity.json");
        URLRequesterImpl requester = new URLRequesterImpl();
        byte[] awsIdentity = requester.get(url, URLRequester.INFINITE_DURATION);
        assertThat(awsIdentity.length).isEqualTo(476);
    }

    @Test
    public void testReadFromHttpURL()
            throws IOException
    {
        URL url = URI.create("http://google.com").toURL();
        URLRequesterImpl requester = new URLRequesterImpl();
        byte[] awsIdentity = requester.get(url, URLRequester.INFINITE_DURATION);
        assertThat(awsIdentity.length).isGreaterThan(0);
    }

    @Test
    public void testConnectTimeoutFromHttpURL()
            throws MalformedURLException
    {
        URL url = URI.create("http://this.should.not.resolve.to.any.ip.ff8d8udofuiy9385yljhngflg948ueloirje.com").toURL();
        URLRequesterImpl requester = new URLRequesterImpl();
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> requester.get(url, URLRequester.MINIMUM_DURATION));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "CONTINUOUS_INTEGRATION", matches = "true")
    public void testGetAWSMetadataServiceAuthToken()
            throws IOException
    {
        URLRequesterImpl requester = new URLRequesterImpl();
        byte[] response = requester.put(
                URI.create(METADATA_SERVICE_BASE_URL + "/latest/api/token").toURL(),
                Duration.ofSeconds(10),
                ImmutableMap.of("X-aws-ec2-metadata-token-ttl-seconds", "180"));

        assertThat(response).isNotNull();
        assertThat(response.length).isGreaterThan(1);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "CONTINUOUS_INTEGRATION", matches = "true")
    public void testGetAWSMetadataServiceInstanceId()
            throws IOException
    {
        URLRequesterImpl requester = new URLRequesterImpl();

        String token = new String(requester.put(
                URI.create(METADATA_SERVICE_BASE_URL + "/latest/api/token").toURL(),
                Duration.ofSeconds(10),
                ImmutableMap.of("X-aws-ec2-metadata-token-ttl-seconds", "180")),
                StandardCharsets.UTF_8);

        String instanceId = new String(requester.get(
                URI.create(METADATA_SERVICE_BASE_URL + "/latest/meta-data/instance-id").toURL(),
                Duration.ofSeconds(10),
                ImmutableMap.of("X-aws-ec2-metadata-token", token)),
                StandardCharsets.UTF_8);

        // In CICD environment we expect that RUNNER_NAME env variable is set and contains EC2 instance id
        String runnerName = System.getenv("RUNNER_NAME");
        assertThat(runnerName).isNotBlank();

        assertThat(instanceId).isNotNull();
        assertThat(instanceId).isSubstringOf(runnerName);
    }
}
