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

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestAWSMarketplaceLicenseProvider
{
    private static final String METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH = "/latest/dynamic/instance-identity/";

    private static final byte[] exampleInstanceIdentityResponse = "document\npkcs7\nrsa2048\nsignature".getBytes(UTF_8);
    private static byte[] identityStarburstAlluxioJson;
    private static final byte[] signature = "signature".getBytes(UTF_8);
    private static URLRequester defaultTestRequester;

    @BeforeAll
    public static void setUp()
            throws IOException
    {
        URL identityStarburstURL = getResource(TestAWSMarketplaceLicenseProvider.class, "test-marketplace_starburst_identity.json");
        URL identityStarburstAlluxioURL = getResource(TestAWSMarketplaceLicenseProvider.class, "test-marketplace_starburst_alluxio_identity.json");

        identityStarburstAlluxioJson = toByteArray(identityStarburstAlluxioURL);

        defaultTestRequester = TestingURLRequester.builder()
                .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH, exampleInstanceIdentityResponse)
                .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "document", toByteArray(identityStarburstURL))
                .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "signature", signature)
                .build();
    }

    @Test
    public void testReturnsEmptyOptionWhenInstanceIdentityVerifierFails()
    {
        final String message = "You shall not pass!";
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {
                    throw new VerificationException(message);
                },
                ImmutableSet.of("test_marketplace_code"),
                defaultTestRequester);
        assertThatExceptionOfType(VerificationException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage(message);
    }

    @Test
    public void testReturnsEmptyOptionWhenMarketplaceProductCodeDoesNotMatch()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("test_marketplace_code"),
                defaultTestRequester);
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsEmptyOptionWhenMarketplaceProductCodesIsNull()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("test_marketplace_code"),
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH, exampleInstanceIdentityResponse)
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "document",
                                "{\"marketplaceProductCodes\": null, \"region\": \"us-east-1\"}".getBytes(UTF_8))
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "signature", signature)
                        .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testReturnsEmptyOptionIfInstanceIdentityRootDownloadRaisesIOException()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("test_marketplace_code"),
                TestingURLRequester.builder()
                        .withExceptionOnPut(new IOException("Just kidding"))
                        .build());
        assertThat(licenseProvider.getLicense()).isEmpty();
    }

    @Test
    public void testRaisesTrinoExceptionIfIdentityDocumentDownloadRaisesIoException()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("test_marketplace_code"),
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH, exampleInstanceIdentityResponse)
                        .withExceptionOnDownloadPathMismatch(new IOException("Just kidding"))
                        .build());
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Problem encountered while checking the AWS instance identity document")
                .havingCause()
                .isInstanceOf(IOException.class)
                .withMessage("Just kidding");
    }

    @Test
    public void testWrapsJsonProblemsInTrinoException()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("test_marketplace_code"),
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH, exampleInstanceIdentityResponse)
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "document",
                                "]pofigpsoi3p5938r-09gpfgjf.m,n{}341344<>?>34.,".getBytes(UTF_8))
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "signature", signature)
                        .build());
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(licenseProvider::getLicense)
                .withMessage("Problem encountered while checking the AWS instance identity document");
    }

    @Test
    public void testReturnsLicenseWhenMarketplaceProductCodeMatchesStarburstPresto()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("yh32yzm07ctyg8b4gpsk127n"),
                defaultTestRequester);
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS, LocalDateTime.MAX));
        assertThat(licenseProvider.getLicense().orElseThrow().getHash()).isEmpty();
        assertThat(licenseProvider.getFileHandle()).isEmpty();
    }

    @Test
    public void testReturnsLicenseWhenMarketplaceProductCodeMatchesStarburstPrestoAlluxio()
    {
        LicenseProvider licenseProvider = new AWSMarketplaceLicenseProvider(
                (identity, base64Signature) -> {},
                ImmutableSet.of("6asvaypvo3rvqrp89gypcdsfj"),
                TestingURLRequester.builder()
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH, exampleInstanceIdentityResponse)
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "document", identityStarburstAlluxioJson)
                        .withDownloadResponse(METADATA_SERVICE_INSTANCE_IDENTITY_BASE_PATH + "signature", signature)
                        .build());
        assertThat(licenseProvider.getLicense()).contains(License.unsigned("200442618260", LicenseType.AWS, LocalDateTime.MAX));
        assertThat(licenseProvider.getFileHandle()).isEmpty();
    }
}
