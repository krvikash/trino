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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // because ManagedKubernetesLicenseProvider caches the license in a static field
final class TestManagedKubernetesLicenseProvider
{
    private static final String METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH = "/latest/dynamic/instance-identity/";

    private static final Map<String, String> ENVIRONMENT = ImmutableMap.of(
            "ENABLE_K8S_LICENSE_PROVIDER", "true",
            "K8S_LICENSE_PROVIDER_MAX_RETRY", "3");

    private static byte[] licenseCheckoutFailureJson;
    private static byte[] licenseCheckoutSuccessJson;
    private static byte[] licenseCheckoutCrashJson;
    private static byte[] licenseCheckoutCloudMismatchJson;

    @BeforeAll
    static void setUp()
            throws IOException
    {
        URL licenseCheckoutFailureURL = getResource(TestManagedKubernetesLicenseProvider.class, "test-license-checkout-failure.json");
        URL licenseCheckoutSuccessURL = getResource(TestManagedKubernetesLicenseProvider.class, "test-license-checkout-success.json");
        URL licenseCheckoutCrashURL = getResource(TestManagedKubernetesLicenseProvider.class, "test-license-checkout-crash.json");
        URL licenseCheckoutCloudMismatchURL = getResource(TestManagedKubernetesLicenseProvider.class, "test-license-checkout-mismatch-cloud.json");

        licenseCheckoutFailureJson = toByteArray(licenseCheckoutFailureURL);
        licenseCheckoutSuccessJson = toByteArray(licenseCheckoutSuccessURL);
        licenseCheckoutCrashJson = toByteArray(licenseCheckoutCrashURL);
        licenseCheckoutCloudMismatchJson = toByteArray(licenseCheckoutCloudMismatchURL);
    }

    @BeforeEach
    void reset()
    {
        ManagedKubernetesLicenseProvider.setManagedKubernetesLicense(null);
    }

    @Test
    void testRaisesTrinoExceptionIfLicenseEndpointNotAvailable()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withExceptionOnDownload(new IOException("Download failed, host unreachable"))
                        .build(),
                ENVIRONMENT);
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Starburst Enterprise Managed Kubernetes License Verifier failed to start");
    }

    @Test
    void testReturnsEmptyIfLicenseProviderIsDisabled()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "/license", licenseCheckoutSuccessJson)
                        .build(),
                ImmutableMap.<String, String>builder()
                        .putAll(ENVIRONMENT)
                        .put("ENABLE_K8S_LICENSE_PROVIDER", "false")
                        .buildKeepingLast());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    void testReturnsEmptyIfLicenseProviderEnvVarIsNotSet()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "/license", licenseCheckoutSuccessJson)
                        .build(),
                ImmutableMap.of("K8S_LICENSE_PROVIDER_MAX_RETRY", "3"));
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    void testReturnsEmptyIfLicenseCheckoutFail()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse("/license", licenseCheckoutFailureJson)
                        .build(),
                ENVIRONMENT);
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    void testReturnsEmptyIfCloudNotSupported()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse("/license", licenseCheckoutCloudMismatchJson)
                        .build(),
                ENVIRONMENT);
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    void testReturnsLicenseIfAWSLicenseCheckoutReturnsSuccess()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse("/license", licenseCheckoutSuccessJson)
                        .build(),
                ENVIRONMENT);
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS_EKS, LocalDateTime.MAX));
        assertThat(licenseProvider.getLicense().orElseThrow().getHash()).isEmpty();

        // Verify cashing - no external services will be called as license was previously created (verification that throws exception will not be called.
        licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse("/license", licenseCheckoutFailureJson)
                        .build(),
                ENVIRONMENT);
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS_EKS, LocalDateTime.MAX));
        assertThat(licenseProvider.getLicense().orElseThrow().getHash()).isEmpty();
    }

    @Test
    void testRaisesTrinoExceptionIfLicenseEndpointCrashes()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(
                TestingURLRequester.builder()
                        .withDownloadResponse("/license", licenseCheckoutCrashJson)
                        .build(),
                ENVIRONMENT);
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Internal Starburst Enterprise Managed Kubernetes License Verifier error");
    }
}
