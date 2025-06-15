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

import org.junit.jupiter.api.Test;

import java.net.URL;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAWSInstanceIdentityVerifier
{
    @Test
    public void testVerifyWithAWSIdentity()
            throws Exception
    {
        InstanceIdentityVerifier verifier = new AWSInstanceIdentityVerifier();

        URL identityURL = getResource(getClass(), "test-aws_identity.json");
        URL signatureURL = getResource(getClass(), "test-aws_identity.signature");
        byte[] identity = toByteArray(identityURL);
        byte[] base64Signature = toByteArray(signatureURL);

        verifier.verify(identity, base64Signature);
    }

    @Test
    public void testVerifyWithAWSIdentityGov()
            throws Exception
    {
        InstanceIdentityVerifier verifier = new AWSInstanceIdentityVerifier();

        URL identityURL = getResource(getClass(), "test-aws_identity-gov.json");
        URL signatureURL = getResource(getClass(), "test-aws_identity-gov.signature");
        byte[] identity = toByteArray(identityURL);
        byte[] base64Signature = toByteArray(signatureURL);

        verifier.verify(identity, base64Signature);
    }

    @Test
    public void testVerifyWithMarketplaceIdentity()
            throws Exception
    {
        InstanceIdentityVerifier verifier = new AWSInstanceIdentityVerifier();

        URL identityURL = getResource(getClass(), "test-marketplace_starburst_identity.json");
        URL signatureURL = getResource(getClass(), "test-marketplace_identity.signature");
        byte[] identity = toByteArray(identityURL);
        byte[] base64Signature = toByteArray(signatureURL);

        verifier.verify(identity, base64Signature);
    }

    @Test
    public void testVerifyWithFakedMarketplaceIdentity()
            throws Exception
    {
        InstanceIdentityVerifier verifier = new AWSInstanceIdentityVerifier();

        URL identityURL = getResource(getClass(), "test-marketplace_starburst_identity.json");
        URL signatureURL = getResource(getClass(), "test-aws_identity.signature");
        byte[] identity = toByteArray(identityURL);
        byte[] base64Signature = toByteArray(signatureURL);

        assertThatThrownBy(() -> verifier.verify(identity, base64Signature))
                .isInstanceOf(VerificationException.class)
                .hasMessage("AWS instance identity document verification failed");
    }

    @Test
    public void testVerifyWithIncorrectSignature()
            throws Exception
    {
        InstanceIdentityVerifier verifier = new AWSInstanceIdentityVerifier();

        URL identityURL = getResource(getClass(), "test-aws_identity.json");
        URL signatureURL = getResource(getClass(), "test-marketplace_identity.signature");
        byte[] identity = toByteArray(identityURL);
        byte[] base64Signature = toByteArray(signatureURL);

        assertThatThrownBy(() -> verifier.verify(identity, base64Signature))
                .isInstanceOf(VerificationException.class)
                .hasMessage("AWS instance identity document verification failed");
    }
}
