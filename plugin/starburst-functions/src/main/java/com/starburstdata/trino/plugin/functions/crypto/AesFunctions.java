/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.crypto;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;

import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Locale;

import static com.starburstdata.trino.plugin.functions.crypto.AesKeys.BLOCK_LENGTH;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * Implementations of the {@code crypto} schema functions. The SQL-visible metadata and the
 * method handles live in {@link CryptoFunctions}; the arity of each method here must match
 * the signatures declared there.
 * <p>
 * Supported modes are GCM (default, authenticated) and CBC. The ciphertext envelope produced by
 * {@code aes_encrypt} prepends the initialization vector: 12 bytes for GCM, 16 bytes for CBC.
 * GCM additionally appends a 16-byte authentication tag. When an initialization vector is supplied
 * explicitly to either function it is used directly; when omitted it is generated randomly on
 * encrypt and read from the ciphertext prefix on decrypt.
 * <p>
 * CBC provides no authentication. Use GCM, or verify a message authentication code separately
 * before decrypting with CBC.
 */
public final class AesFunctions
{
    private static final Logger log = Logger.get(AesFunctions.class);

    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int GCM_TAG_LENGTH = 16;
    private static final int CBC_IV_LENGTH = 16;

    private enum Mode
    {
        GCM, CBC;

        private int ivLength()
        {
            return switch (this) {
                case GCM -> GCM_IV_LENGTH;
                case CBC -> CBC_IV_LENGTH;
            };
        }

        private Cipher cipher()
        {
            return switch (this) {
                case GCM -> newCipher("AES/GCM/NoPadding");
                case CBC -> CBC_CIPHER.get();
            };
        }

        private static Mode parse(Slice mode)
        {
            return switch (mode.toStringUtf8().toUpperCase(Locale.ROOT)) {
                case "GCM" -> GCM;
                case "CBC" -> CBC;
                default -> throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Unsupported AES mode '%s'. Supported modes are: GCM, CBC".formatted(mode.toStringUtf8()));
            };
        }
    }

    // GCM: create a new Cipher instance per call. SunJCE's GCM implementation refuses to re-init
    // with the same (key, IV) pair on the same instance, which prevents ThreadLocal reuse.
    // GCM is hardware-accelerated on modern JVMs so per-call allocation is not a concern.
    private static final ThreadLocal<Cipher> CBC_CIPHER = ThreadLocal.withInitial(() -> newCipher("AES/CBC/PKCS5Padding"));
    // SHA1PRNG seeds from OS entropy once per thread, then generates bytes as a PRNG without further syscalls.
    private static final ThreadLocal<SecureRandom> RANDOM = ThreadLocal.withInitial(() -> {
        try {
            return SecureRandom.getInstance("SHA1PRNG");
        }
        catch (NoSuchAlgorithmException e) {
            log.warn(e, "SHA1PRNG is not available; falling back to the platform default SecureRandom");
            return new SecureRandom();
        }
    });

    private AesFunctions() {}

    // aes_encrypt

    public static Slice aesEncrypt(Slice plaintext, Slice key)
    {
        return doEncrypt(plaintext, key, Mode.GCM, null, null);
    }

    public static Slice aesEncrypt(Slice plaintext, Slice key, Slice mode)
    {
        return doEncrypt(plaintext, key, Mode.parse(mode), null, null);
    }

    public static Slice aesEncrypt(Slice plaintext, Slice key, Slice mode, Slice iv)
    {
        return doEncrypt(plaintext, key, Mode.parse(mode), iv, null);
    }

    public static Slice aesEncrypt(Slice plaintext, Slice key, Slice mode, Slice iv, Slice aad)
    {
        return doEncrypt(plaintext, key, Mode.parse(mode), iv, aad);
    }

    // aes_decrypt

    public static Slice aesDecrypt(Slice ciphertext, Slice key)
    {
        return doDecrypt(ciphertext, key, Mode.GCM, null, null);
    }

    public static Slice aesDecrypt(Slice ciphertext, Slice key, Slice mode)
    {
        return doDecrypt(ciphertext, key, Mode.parse(mode), null, null);
    }

    public static Slice aesDecrypt(Slice ciphertext, Slice key, Slice mode, Slice iv)
    {
        return doDecrypt(ciphertext, key, Mode.parse(mode), iv, null);
    }

    public static Slice aesDecrypt(Slice ciphertext, Slice key, Slice mode, Slice iv, Slice aad)
    {
        return doDecrypt(ciphertext, key, Mode.parse(mode), iv, aad);
    }

    // Implementation

    private static Slice doEncrypt(Slice plaintext, Slice key, Mode mode, Slice iv, Slice aad)
    {
        validateAadForMode(mode, aad);

        SecretKey secretKey = AesKeys.secretKey(key);
        int ivLength = mode.ivLength();

        byte[] ivBytes;
        if (iv == null) {
            ivBytes = new byte[ivLength];
            RANDOM.get().nextBytes(ivBytes);
        }
        else {
            ivBytes = validatedIv(iv, ivLength, mode).getBytes();
        }

        try {
            Cipher cipher = mode.cipher();
            switch (mode) {
                case GCM -> {
                    cipher.init(ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, ivBytes));
                    if (aad != null) {
                        cipher.updateAAD(aad.byteArray(), aad.byteArrayOffset(), aad.length());
                    }
                }
                case CBC -> cipher.init(ENCRYPT_MODE, secretKey, new IvParameterSpec(ivBytes));
            }
            // Output envelope: iv || ciphertext (GCM appends a 16-byte authentication tag)
            byte[] output = new byte[ivLength + cipher.getOutputSize(plaintext.length())];
            System.arraycopy(ivBytes, 0, output, 0, ivLength);
            cipher.doFinal(plaintext.byteArray(), plaintext.byteArrayOffset(), plaintext.length(), output, ivLength);
            return Slices.wrappedBuffer(output);
        }
        catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
            // Attributable to the key or initialization vector the caller supplied, so report it as a user error.
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "AES encryption failed: " + e.getMessage(), e);
        }
        catch (GeneralSecurityException e) {
            // Unreachable given the validation above; getting here means that validation and the JCE
            // provider disagree, which is a defect rather than bad input.
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "AES encryption failed: " + e.getMessage(), e);
        }
    }

    private static Slice doDecrypt(Slice ciphertext, Slice key, Mode mode, Slice iv, Slice aad)
    {
        validateAadForMode(mode, aad);

        SecretKey secretKey = AesKeys.secretKey(key);
        IvAndPayload input = parseIv(ciphertext, mode, iv);
        Slice payload = input.payload();

        try {
            Cipher cipher = mode.cipher();
            switch (mode) {
                case GCM -> {
                    cipher.init(DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, input.iv().getBytes()));
                    if (aad != null) {
                        cipher.updateAAD(aad.byteArray(), aad.byteArrayOffset(), aad.length());
                    }
                }
                case CBC -> cipher.init(DECRYPT_MODE, secretKey, new IvParameterSpec(input.iv().getBytes()));
            }
            return Slices.wrappedBuffer(cipher.doFinal(payload.byteArray(), payload.byteArrayOffset(), payload.length()));
        }
        catch (AEADBadTagException e) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "AES-GCM decryption failed: authentication tag mismatch, caused by a wrong key, wrong associated data, or tampered ciphertext");
        }
        catch (GeneralSecurityException e) {
            // Throw a user error, suppressible with TRY(), so that a single undecryptable row yields NULL instead of failing the query.
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "AES decryption failed: " + e.getMessage(), e);
        }
    }

    private record IvAndPayload(Slice iv, Slice payload) {}

    /**
     * Splits a ciphertext into the initialization vector to decrypt with and the payload to decrypt.
     * When {@code iv} is null the vector is read from the ciphertext prefix, where {@code aes_encrypt}
     * writes it; when {@code iv} is supplied the whole ciphertext is payload, which is how values
     * whose vector is stored apart from the ciphertext are read.
     */
    private static IvAndPayload parseIv(Slice ciphertext, Mode mode, Slice iv)
    {
        int ivLength = mode.ivLength();

        if (iv != null) {
            // IV provided externally (e.g. from a Laravel envelope stored separately from the value)
            if (mode == Mode.CBC && (ciphertext.length() == 0 || (ciphertext.length() % BLOCK_LENGTH) != 0)) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Ciphertext length must be a non-zero multiple of %s bytes for CBC, but is %s bytes".formatted(BLOCK_LENGTH, ciphertext.length()));
            }
            return new IvAndPayload(validatedIv(iv, ivLength, mode), ciphertext);
        }

        // IV is embedded as the ciphertext prefix
        switch (mode) {
            case GCM -> {
                int minimumLength = ivLength + GCM_TAG_LENGTH;
                if (ciphertext.length() < minimumLength) {
                    throw new TrinoException(
                            INVALID_FUNCTION_ARGUMENT,
                            "Ciphertext must be at least %s bytes long for GCM, but is %s bytes long".formatted(minimumLength, ciphertext.length()));
                }
            }
            case CBC -> {
                int payloadLength = ciphertext.length() - ivLength;
                if (payloadLength <= 0 || (payloadLength % BLOCK_LENGTH) != 0) {
                    throw new TrinoException(
                            INVALID_FUNCTION_ARGUMENT,
                            "Ciphertext length after IV prefix must be a non-zero multiple of %s bytes for CBC, but is %s bytes".formatted(
                                    BLOCK_LENGTH, Math.max(0, payloadLength)));
                }
            }
        }

        return new IvAndPayload(ciphertext.slice(0, ivLength), ciphertext.slice(ivLength, ciphertext.length() - ivLength));
    }

    private static void validateAadForMode(Mode mode, Slice aad)
    {
        if (mode != Mode.GCM && aad != null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "AAD is only supported in GCM mode");
        }
    }

    private static Slice validatedIv(Slice iv, int expectedLength, Mode mode)
    {
        if (iv.length() != expectedLength) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Initialization vector for %s must be %s bytes long, but is %s bytes long".formatted(mode, expectedLength, iv.length()));
        }
        return iv;
    }

    private static Cipher newCipher(String transformation)
    {
        try {
            return Cipher.getInstance(transformation);
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, transformation + " is not available in this JVM", e);
        }
    }
}
