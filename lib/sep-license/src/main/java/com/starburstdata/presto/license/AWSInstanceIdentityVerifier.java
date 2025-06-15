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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.license.AWSPartition.AWS;
import static com.starburstdata.presto.license.AWSPartition.AWS_US_GOV;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class AWSInstanceIdentityVerifier
        implements InstanceIdentityVerifier
{
    private static final Map<AWSPartition, URL> AWS_PARTITION_CERTIFICATES = ImmutableMap.of(
            AWS, getResource(AWSInstanceIdentityVerifier.class, "aws_public_key.rsa.cert"),
            // use the us-gov-west-1 certificate as the default for any new/unknown GovCloud regions
            // the certs for us-gov-west-1 and us-gov-east-1 work in both regions
            AWS_US_GOV, getResource(AWSInstanceIdentityVerifier.class, "aws_public_key.gov-west-1.rsa.cert"));
    private static final Map<AWSRegion, URL> AWS_REGION_CERTIFICATES = ImmutableMap.of(
            AWSRegion.US_GOV_EAST_1, getResource(AWSInstanceIdentityVerifier.class, "aws_public_key.gov-east-1.rsa.cert"),
            AWSRegion.US_GOV_WEST_1, getResource(AWSInstanceIdentityVerifier.class, "aws_public_key.gov-west-1.rsa.cert"));
    private static final JsonCodec<AWSIdentityDocument> AWS_IDENTITY_DOCUMENT_JSON_CODEC = new JsonCodecFactory().jsonCodec(AWSIdentityDocument.class);

    private final String algorithm;
    private final Map<AWSPartition, Certificate> partitionCertificates;
    private final Map<AWSRegion, Certificate> regionCertificates;

    AWSInstanceIdentityVerifier()
    {
        this(AWS_PARTITION_CERTIFICATES, AWS_REGION_CERTIFICATES, "SHA256WithRSA");
    }

    @VisibleForTesting
    AWSInstanceIdentityVerifier(Map<AWSPartition, URL> partitionCertificateURLs, Map<AWSRegion, URL> regionCertificateURLs, String algorithm)
    {
        this.algorithm = requireNonNull(algorithm, "algorithm is null");
        CertificateFactory certificateFactory;
        try {
            certificateFactory = CertificateFactory.getInstance("X.509");
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error getting certificate factory or signature instance", e);
        }
        partitionCertificates = readCertificates(certificateFactory, partitionCertificateURLs);
        regionCertificates = readCertificates(certificateFactory, regionCertificateURLs);
    }

    private <T> Map<T, Certificate> readCertificates(CertificateFactory certificateFactory, Map<T, URL> urls)
    {
        ImmutableMap.Builder<T, Certificate> certificateBuilder = ImmutableMap.builder();
        for (Map.Entry<T, URL> entry : urls.entrySet()) {
            T key = entry.getKey();
            URL certificateURL = requireNonNull(entry.getValue(), format("Certificate URL for %s is null", key));
            try (InputStream inputStream = certificateURL.openStream()) {
                certificateBuilder.put(key, certificateFactory.generateCertificate(inputStream));
            }
            catch (GeneralSecurityException | IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error while creating AWSInstanceIdentityVerifier", e);
            }
        }
        return certificateBuilder.buildOrThrow();
    }

    @Override
    public void verify(byte[] identity, byte[] base64Signature)
            throws VerificationException
    {
        AWSIdentityDocument identityDocument = AWS_IDENTITY_DOCUMENT_JSON_CODEC.fromJson(identity);
        Certificate awsCertificate = null;
        AWSRegion region;
        try {
            region = AWSRegion.fromName(identityDocument.getRegion());
            awsCertificate = regionCertificates.get(region);
        }
        catch (IllegalArgumentException _) {
            // ignore
        }
        if (awsCertificate == null) {
            AWSPartition partition = AWSPartition.fromRegion(identityDocument.getRegion());
            // unknown partitions are mapped to the main one, AWS, so this should always be not null
            awsCertificate = partitionCertificates.get(partition);
        }
        try {
            PublicKey publicKey = awsCertificate.getPublicKey();
            byte[] signatureBytes = Base64.getMimeDecoder().decode(base64Signature);
            Signature signature = Signature.getInstance(algorithm);
            signature.initVerify(publicKey);
            signature.update(identity);
            if (!signature.verify(signatureBytes)) {
                throw new VerificationException("AWS instance identity document verification failed");
            }
        }
        catch (InvalidKeyException | SignatureException | NoSuchAlgorithmException e) {
            throw new VerificationException("AWS identity verification failed", e);
        }
    }
}
