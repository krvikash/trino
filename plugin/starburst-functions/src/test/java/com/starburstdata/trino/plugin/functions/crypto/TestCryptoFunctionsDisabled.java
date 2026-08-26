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

import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.addStarburstCatalog;
import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.cryptoSession;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * With {@code crypto.enabled} left at its default of {@code false}, the crypto schema and its
 * functions must be absent entirely rather than present-but-denied.
 */
@TestInstance(PER_CLASS)
public class TestCryptoFunctionsDisabled
{
    private static final String KEY = "X'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        QueryRunner queryRunner = new StandaloneQueryRunner(cryptoSession());
        addStarburstCatalog(queryRunner, false);
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testCryptoSchemaIsNotListed()
    {
        assertThat(assertions.execute("SHOW SCHEMAS FROM starburst").getOnlyColumnAsSet())
                .doesNotContain("crypto")
                .contains("ai", "io");
    }

    @Test
    public void testFunctionsDoNotResolve()
    {
        assertThat(assertions.query("SELECT starburst.crypto.aes_encrypt(to_utf8('phi'), %s)".formatted(KEY)))
                .failure()
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }
}
