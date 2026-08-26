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

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.addStarburstCatalog;
import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.cryptoSession;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAesGcmFunctions
{
    private static final String KEY_256 = "X'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";
    private static final String KEY_128 = "X'000102030405060708090a0b0c0d0e0f'";
    private static final String KEY_192 = "X'000102030405060708090a0b0c0d0e0f1011121314151617'";
    private static final String AAD = "to_utf8('account-1')";
    // A fixed 12-byte IV for deterministic-mode tests
    private static final String FIXED_IV = "X'aabbccddeeff001122334455'";

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(cryptoSession());
        addStarburstCatalog(queryRunner);
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testRoundTrip()
    {
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('protected health information'), %s), %s))"
                        .formatted(KEY_256, KEY_256)))
                .hasType(VARCHAR)
                .isEqualTo("protected health information");
    }

    @Test
    public void testRoundTripWithExplicitMode()
    {
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM'), %s, 'GCM'))"
                        .formatted(KEY_256, KEY_256)))
                .hasType(VARCHAR)
                .isEqualTo("value");
    }

    @Test
    public void testRoundTripWithAad()
    {
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', null, %s), %s, 'GCM', null, %s))"
                        .formatted(KEY_256, AAD, KEY_256, AAD)))
                .hasType(VARCHAR)
                .isEqualTo("value");
    }

    @Test
    public void testRoundTripForAllKeyLengths()
    {
        for (String key : new String[] {KEY_128, KEY_192, KEY_256}) {
            assertThat(assertions.expression(
                    "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s), %s))".formatted(key, key)))
                    .hasType(VARCHAR)
                    .isEqualTo("value");
        }
    }

    @Test
    public void testRoundTripOfEmptyPlaintext()
    {
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(X'', %s), %s))".formatted(KEY_256, KEY_256)))
                .hasType(VARCHAR)
                .isEqualTo("");
    }

    @Test
    public void testEncryptionIsNotDeterministicByDefault()
    {
        assertThat(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), %s) <> starburst.crypto.aes_encrypt(to_utf8('value'), %s)"
                        .formatted(KEY_256, KEY_256)))
                .isEqualTo(true);
    }

    @Test
    public void testEncryptionIsDeterministicWhenIvIsSupplied()
    {
        assertThat(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', %s) = starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', %s)"
                        .formatted(KEY_256, FIXED_IV, KEY_256, FIXED_IV)))
                .isEqualTo(true);
    }

    @Test
    public void testDecryptWithExternalIv()
    {
        // Encrypt with explicit IV, decrypt by stripping the prefix and supplying IV directly
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(substr(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', %s), 13), %s, 'GCM', %s))"
                        .formatted(KEY_256, FIXED_IV, KEY_256, FIXED_IV)))
                .hasType(VARCHAR)
                .isEqualTo("value");
    }

    @Test
    public void testCiphertextLayout()
    {
        // 12-byte IV, then the encrypted value, then a 16-byte authentication tag
        assertThat(assertions.expression(
                "length(starburst.crypto.aes_encrypt(to_utf8('12345'), %s))".formatted(KEY_256)))
                .isEqualTo(12L + 5 + 16);
    }

    @Test
    public void testNullArgumentReturnsNull()
    {
        assertThat(assertions.expression("starburst.crypto.aes_encrypt(null, %s)".formatted(KEY_256)))
                .isNull(VARBINARY);
        assertThat(assertions.expression("starburst.crypto.aes_encrypt(to_utf8('value'), null)"))
                .isNull(VARBINARY);
        assertThat(assertions.expression("starburst.crypto.aes_decrypt(null, %s)".formatted(KEY_256)))
                .isNull(VARBINARY);
        assertThat(assertions.expression("starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s), null)".formatted(KEY_256)))
                .isNull(VARBINARY);
    }

    @Test
    public void testDecryptWithWrongKey()
    {
        String otherKey = "X'ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s), %s)".formatted(KEY_256, otherKey))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("authentication tag mismatch");
    }

    @Test
    public void testDecryptWithWrongAad()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', null, %s), %s, 'GCM', null, to_utf8('account-2'))"
                        .formatted(KEY_256, AAD, KEY_256))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("authentication tag mismatch");
    }

    @Test
    public void testDecryptTamperedCiphertext()
    {
        // Zeroes the 12-byte IV prefix — always detectable without relying on a specific byte value
        assertTrinoExceptionThrownBy(assertions.expression(
                """
                starburst.crypto.aes_decrypt(
                    concat(
                        X'000000000000000000000000',
                        substr(starburst.crypto.aes_encrypt(to_utf8('value'), %s), 13)),
                    %s)""".formatted(KEY_256, KEY_256))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("authentication tag mismatch");
    }

    @Test
    public void testDecryptTruncatedCiphertext()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(X'0011223344556677', %s)".formatted(KEY_256))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Ciphertext must be at least 28 bytes long for GCM, but is 8 bytes long");
    }

    @Test
    public void testInvalidKeyLength()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), X'0001020304')")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Key must be 16, 24 or 32 bytes long for AES-128, AES-192 or AES-256, but is 5 bytes long");
    }

    @Test
    public void testInvalidMode()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'ECB')".formatted(KEY_256))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Unsupported AES mode 'ECB'");
    }

    @Test
    public void testInvalidIvLength()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', X'aabbccdd')".formatted(KEY_256))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Initialization vector for GCM must be 12 bytes long, but is 4 bytes long");
    }

    /**
     * Verifies that TRY() returns NULL on decryption failure, enabling BigQuery SAFE.-style
     * behaviour in dbt macros where one undecryptable row must not fail the whole query.
     */
    @Test
    public void testTryYieldsNullOnDecryptionFailure()
    {
        assertThat(assertions.expression(
                "TRY(starburst.crypto.aes_decrypt(X'0011223344556677', %s))".formatted(KEY_256)))
                .isNull(VARBINARY);

        assertThat(assertions.expression(
                "TRY(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'GCM', null, %s), %s, 'GCM', null, to_utf8('account-2')))"
                        .formatted(KEY_256, AAD, KEY_256)))
                .isNull(VARBINARY);
    }
}
