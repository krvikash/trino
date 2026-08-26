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

import io.trino.Session;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.addStarburstCatalog;
import static com.starburstdata.trino.plugin.functions.crypto.CryptoQueryRunner.cryptoSession;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.SqlPath.buildPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Covers how the crypto functions are addressed, as distinct from what they compute: they live in
 * the {@code crypto} schema of the {@code starburst} catalog, and are reachable from nowhere else.
 */
@TestInstance(PER_CLASS)
public class TestCryptoFunctionsResolution
{
    private static final String KEY = "X'000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'";

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
    public void testCryptoSchemaIsListed()
    {
        assertThat(assertions.query("SHOW SCHEMAS FROM starburst"))
                .containsAll("VALUES VARCHAR 'crypto'");
    }

    @Test
    public void testAllOverloadsAreListedInCryptoSchema()
    {
        MaterializedResult result = assertions.execute("SHOW FUNCTIONS FROM starburst.crypto");
        assertThat(result.getMaterializedRows())
                .hasSize(8)
                .allSatisfy(row -> assertThat((String) row.getField(0)).isIn("aes_encrypt", "aes_decrypt"));
    }

    @Test
    public void testResolvesInCryptoSchema()
    {
        assertThat(assertions.expression(
                "from_utf8(starburst.crypto.aes_decrypt(starburst.crypto.aes_encrypt(to_utf8('phi'), %s), %s))".formatted(KEY, KEY)))
                .hasType(VARCHAR)
                .isEqualTo("phi");
    }

    @Test
    public void testDoesNotResolveInAiSchema()
    {
        assertThat(assertions.query("SELECT starburst.ai.aes_encrypt(to_utf8('phi'), %s)".formatted(KEY)))
                .failure()
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testDoesNotResolveAsBuiltin()
    {
        assertThat(assertions.query("SELECT aes_encrypt(to_utf8('phi'), %s)".formatted(KEY)))
                .failure()
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    /**
     * Setting {@code sql.path} to the crypto schema restores unqualified calls, which is how the dbt
     * macros are expected to reference these functions.
     */
    @Test
    public void testResolvesUnqualifiedOnSqlPath()
    {
        Session session = Session.builder(cryptoSession())
                .setPath(buildPath("starburst.crypto", Optional.empty()))
                .build();
        assertThat(assertions.query(
                session,
                "SELECT from_utf8(aes_decrypt(aes_encrypt(to_utf8('phi'), %s), %s))".formatted(KEY, KEY)))
                .matches("VALUES VARCHAR 'phi'");
    }
}
