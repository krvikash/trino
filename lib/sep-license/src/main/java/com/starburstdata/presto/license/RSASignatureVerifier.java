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
import com.google.common.io.Resources;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

class RSASignatureVerifier
        implements SignatureVerifier
{
    private final String algorithm;
    private final PublicKey publicKey;

    RSASignatureVerifier()
    {
        this(Resources.getResource(RSASignatureVerifier.class, "presto-license_public_key.der"));
    }

    @VisibleForTesting
    RSASignatureVerifier(URL publicKeyURL)
    {
        this(publicKeyURL, "SHA512withRSA");
    }

    @VisibleForTesting
    RSASignatureVerifier(URL publicKeyURL, String algorithm)
    {
        this.algorithm = requireNonNull(algorithm, "algorithm is null");
        try {
            byte[] publicKeyData = Resources.toByteArray(publicKeyURL);
            EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(publicKeyData);
            publicKey = KeyFactory.getInstance("RSA").generatePublic(publicKeySpec);
        }
        catch (NoSuchAlgorithmException | IOException | InvalidKeySpecException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Problem encountered while creating RSASignatureVerifier", e);
        }
    }

    @Override
    public void verify(byte[] license, byte[] signature)
            throws VerificationException
    {
        try {
            Signature rsa = Signature.getInstance(algorithm);
            rsa.initVerify(publicKey);
            rsa.update(license);
            if (!rsa.verify(signature)) {
                throw new VerificationException("No valid license found");
            }
        }
        catch (InvalidKeyException | SignatureException | NoSuchAlgorithmException ex) {
            throw new VerificationException("No valid license found", ex);
        }
    }
}
