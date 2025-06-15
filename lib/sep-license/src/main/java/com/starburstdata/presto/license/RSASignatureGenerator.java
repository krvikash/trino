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
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import static com.google.common.io.Resources.toByteArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

class RSASignatureGenerator
        implements SignatureGenerator
{
    private final String algorithm;
    private final PrivateKey privateKey;

    RSASignatureGenerator(URL privateKeyURL)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        this(privateKeyURL, "SHA512withRSA");
    }

    @VisibleForTesting
    RSASignatureGenerator(URL privateKeyURL, String algorithm)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        this.algorithm = requireNonNull(algorithm, "algorithm is null");
        byte[] privateKeyData = toByteArray(privateKeyURL);
        EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(privateKeyData);
        privateKey = KeyFactory.getInstance("RSA").generatePrivate(privKeySpec);
    }

    @Override
    public byte[] sign(byte[] license)
            throws TrinoException
    {
        try {
            Signature rsa = Signature.getInstance(algorithm);
            rsa.initSign(privateKey);
            rsa.update(license);
            return rsa.sign();
        }
        catch (InvalidKeyException | SignatureException | NoSuchAlgorithmException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
