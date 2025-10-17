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

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@ExtendWith(SystemStubsExtension.class)
@Execution(SAME_THREAD) // because of SystemStub
public class TestManagedKubernetesLicenseProvider
{
    private static final String METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH = "/latest/dynamic/instance-identity/";

    private static byte[] licenseCheckoutFailureJson;
    private static byte[] licenseCheckoutSuccessJson;
    private static byte[] licenseCheckoutCrashJson;
    private static byte[] licenseCheckoutCloudMismatchJson;

    @SystemStub
    private final EnvironmentVariables variables = new EnvironmentVariables();

    @BeforeAll
    public static void setUp()
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
    public void reset()
    {
        ManagedKubernetesLicenseProvider.setManagedKubernetesLicense(null);
        variables.set("ENABLE_K8S_LICENSE_PROVIDER", "true");
        variables.set("K8S_LICENSE_PROVIDER_MAX_RETRY", "3");
    }

    @Test
    public void testRaisesTrinoExceptionIfLicenseEndpointNotAvailable()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withExceptionOnDownload(new IOException("Download failed, host unreachable"))
                .build());
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Starburst Enterprise Managed Kubernetes License Verifier failed to start");
    }

    @Test
    public void testReturnsEmptyIfLicenseProviderIsDisabled()
    {
        variables.set("ENABLE_K8S_LICENSE_PROVIDER", "false");
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "/license", licenseCheckoutSuccessJson)
                .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsEmptyIfLicenseProviderEnvVarIsNotSet()
    {
        variables.set("ENABLE_K8S_LICENSE_PROVIDER", null);
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "/license", licenseCheckoutSuccessJson)
                .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsEmptyIfLicenseCheckoutFail()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse("/license", licenseCheckoutFailureJson)
                .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsEmptyIfCloudNotSupported()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse("/license", licenseCheckoutCloudMismatchJson)
                .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsLicenseIfAWSLicenseCheckoutReturnsSuccess()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse("/license", licenseCheckoutSuccessJson)
                .build());
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS_EKS, LocalDateTime.MAX));
        assertThat(licenseProvider.getLicense().orElseThrow().getHash()).isEmpty();

        // Verify cashing - no external services will be called as license was previously created (verification that throws exception will not be called.
        licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse("/license", licenseCheckoutFailureJson)
                .build());
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS_EKS, LocalDateTime.MAX));
        assertThat(licenseProvider.getLicense().orElseThrow().getHash()).isEmpty();
    }

    @Test
    public void testRaisesTrinoExceptionIfLicenseEndpointCrashes()
    {
        LicenseProvider licenseProvider = new ManagedKubernetesLicenseProvider(TestingURLRequester.builder()
                .withDownloadResponse("/license", licenseCheckoutCrashJson)
                .build());
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Internal Starburst Enterprise Managed Kubernetes License Verifier error");
    }
}
