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
public class TestAesCbcFunctions
{
    private static final String KEY = "X'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";
    private static final String PLAINTEXT = "protected health information";

    // Reference vector produced independently by "openssl enc -aes-256-cbc -nosalt" (PKCS#7 padding,
    // openssl's default), used to pin interoperability with other AES-CBC implementations rather
    // than only self-consistency. The 28 byte plaintext pads to 32 bytes, so -nopad cannot produce
    // this vector: it rejects an input length that is not a multiple of the block size.
    private static final String OPENSSL_IV = "X'aabbccddeeff00112233445566778899'";
    private static final String OPENSSL_CIPHERTEXT = "X'fe1ba7467e6c56c1ec1b3a1f2a357d87685195608c932d9511487aafd35ba69d'";

    // The same vector wrapped in the envelope shape that Laravel's "encrypted" cast produces:
    // base64 of a JSON object holding base64 iv, value and mac.
    private static final String LARAVEL_ENVELOPE = "eyJpdiI6InFydk0zZTcvQUJFaU0wUlZabmVJbVE9PSIsInZhbHVlIjoiL2h1blJuNXNW"
            + "c0hzR3pvZktqVjloMmhSbFdDTWt5MlZFVWg2cjlOYnBwMD0iLCJtYWMiOiIzNzVlODQ2M2U2YTU5ZmUxMzAxN2JmMmU4NGY0MjVl"
            + "MGMxNzE2MjEzNTI0ZGM2OTk1MGFjOGRkYzAxMTY0M2E5IiwidGFnIjoiIn0=";

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
        // aes_encrypt embeds a random IV prefix; aes_decrypt extracts it automatically
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('%s'), %s, 'CBC'), %s, 'CBC'))".formatted(PLAINTEXT, KEY, KEY)))
                .hasType(VARCHAR)
                .isEqualTo(PLAINTEXT);
    }

    @Test
    public void testRoundTripWithAllKeyLengths()
    {
        String key128 = "X'000102030405060708090a0b0c0d0e0f'";
        String key192 = "X'000102030405060708090a0b0c0d0e0f1011121314151617'";
        for (String key : new String[] {key128, key192, KEY}) {
            assertThat(assertions.expression(
                    "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'CBC'), %s, 'CBC'))".formatted(key, key)))
                    .hasType(VARCHAR)
                    .isEqualTo("value");
        }
    }

    @Test
    public void testDecryptOpensslVectorWithExternalIv()
    {
        // The openssl ciphertext has no IV prefix; the IV is provided as a separate argument
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(%s, %s, 'CBC', %s))".formatted(OPENSSL_CIPHERTEXT, KEY, OPENSSL_IV)))
                .hasType(VARCHAR)
                .isEqualTo(PLAINTEXT);
    }

    @Test
    public void testCbcCiphertextLayout()
    {
        // 16-byte IV prefix, then PKCS-padded ciphertext; 20 bytes rounds up to 32 (two blocks)
        assertThat(assertions.expression(
                "length(starburst.crypto.aes_encrypt(to_utf8('12345678901234567890'), %s, 'CBC'))".formatted(KEY)))
                .isEqualTo(16L + 32);
    }

    @Test
    public void testNullArgumentReturnsNull()
    {
        assertThat(assertions.expression("starburst.crypto.aes_decrypt(null, %s, 'CBC', %s)".formatted(KEY, OPENSSL_IV)))
                .isNull(VARBINARY);
        assertThat(assertions.expression("starburst.crypto.aes_decrypt(%s, null, 'CBC', %s)".formatted(OPENSSL_CIPHERTEXT, OPENSSL_IV)))
                .isNull(VARBINARY);
    }

    @Test
    public void testWrongKeyFailsOnPadding()
    {
        String otherKey = "X'ff0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(%s, %s, 'CBC', %s)".formatted(OPENSSL_CIPHERTEXT, otherKey, OPENSSL_IV))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("AES decryption failed");
    }

    @Test
    public void testInvalidExternalIvLength()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(%s, %s, 'CBC', X'aabbccddeeff001122334455667788')".formatted(OPENSSL_CIPHERTEXT, KEY))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Initialization vector for CBC must be 16 bytes long, but is 15 bytes long");
    }

    @Test
    public void testInvalidCiphertextLengthWithExternalIv()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(X'0011223344', %s, 'CBC', %s)".formatted(KEY, OPENSSL_IV))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Ciphertext length must be a non-zero multiple of 16 bytes for CBC, but is 5 bytes");
    }

    @Test
    public void testInvalidCiphertextLengthWithEmbeddedIv()
    {
        // 17 bytes total: 16 bytes extracted as IV leaves 1 byte — not a block multiple
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(X'00112233445566778899aabbccddeeff00', %s, 'CBC')".formatted(KEY))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Ciphertext length after IV prefix must be a non-zero multiple of 16 bytes for CBC, but is 1 bytes");
    }

    @Test
    public void testInvalidKeyLength()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_decrypt(%s, X'0001020304', 'CBC', %s)".formatted(OPENSSL_CIPHERTEXT, OPENSSL_IV))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("Key must be 16, 24 or 32 bytes long");
    }

    @Test
    public void testAadWithCbcIsRejected()
    {
        assertTrinoExceptionThrownBy(assertions.expression(
                "starburst.crypto.aes_encrypt(to_utf8('value'), %s, 'CBC', null, to_utf8('aad'))".formatted(KEY))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageContaining("AAD is only supported in GCM mode");
    }

    @Test
    public void testTryYieldsNullOnDecryptionFailure()
    {
        assertThat(assertions.expression(
                "TRY(starburst.crypto.aes_decrypt(X'0011223344', %s, 'CBC', %s))".formatted(KEY, OPENSSL_IV)))
                .isNull(VARBINARY);
    }

    /**
     * Exercises the SQL expression that replaces the Laravel decryption UDF. The IV is extracted
     * from the JSON envelope and passed directly to {@code aes_decrypt} via the optional iv argument.
     */
    @Test
    public void testLaravelEnvelopeExpression()
    {
        assertThat(assertions.expression(laravelDecrypt("'" + LARAVEL_ENVELOPE + "'")))
                .hasType(VARCHAR)
                .isEqualTo(PLAINTEXT);
    }

    @Test
    public void testLaravelEnvelopeExpressionPassesThroughPlaintext()
    {
        assertThat(assertions.expression(laravelDecrypt("'not encrypted at all'")))
                .hasType(VARCHAR)
                .isEqualTo("not encrypted at all");
    }

    @Test
    public void testLaravelEnvelopeExpressionTrimsQuotes()
    {
        assertThat(assertions.expression(laravelDecrypt("'\"" + LARAVEL_ENVELOPE + "\"'")))
                .hasType(VARCHAR)
                .isEqualTo(PLAINTEXT);
    }

    @Test
    public void testLaravelEnvelopeExpressionYieldsNullOnCorruptedEnvelope()
    {
        // Keeps the eyJpdi prefix but carries a truncated value
        String corrupted = "eyJpdiI6InFydk0zZTcvQUJFaU0wUlZabmVJbVE9PSIsInZhbHVlIjoiQUFBQSIsIm1hYyI6IiJ9";
        assertThat(assertions.expression(laravelDecrypt("'" + corrupted + "'")))
                .isNull(VARCHAR);
    }

    /**
     * The SQL that the dbt macro expands to, with the base64-encoded key inlined. The IV is
     * extracted from the JSON envelope's {@code iv} field and passed as the optional iv argument to
     * {@code aes_decrypt}, rather than being embedded in the ciphertext.
     */
    private static String laravelDecrypt(String field)
    {
        String envelope = "from_utf8(from_base64(trim(%s, '\"')))".formatted(field);
        return """
               if(NOT starts_with(trim(%s, '"'), 'eyJpdi'),
                   %s,
                   from_utf8(TRY(starburst.crypto.aes_decrypt(
                       from_base64(json_extract_scalar(%s, '$.value')),
                       from_base64('AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8='),
                       'CBC',
                       from_base64(json_extract_scalar(%s, '$.iv'))))))"""
                .formatted(field, field, envelope, envelope);
    }
}
