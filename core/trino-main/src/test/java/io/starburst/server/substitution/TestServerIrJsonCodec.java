/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.json.JsonCodec;
import io.starburst.materialization.ir.Output;
import io.starburst.server.substitution.TestUtils.SupportedTableId;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.starburst.server.substitution.TestUtils.materialization;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * The IR codec the coordinator injects into the metastore reads a connector identity's primitive component as
 * required and non-null.
 */
@TestInstance(PER_CLASS)
class TestServerIrJsonCodec
{
    private static final String ORDINAL = "\"ordinal\":3";

    private TestingTrinoServer server;
    private JsonCodec<Output> codec;
    private Output plan;
    private String json;

    @BeforeAll
    void setUp()
    {
        server = TestingTrinoServer.create();
        codec = server.getInstance(Key.get(new TypeLiteral<>() {}));
        plan = materialization("mv", new SupportedTableId("source"), new OrdinalColumnId(3)).computationPlanRoot();
        json = codec.toJson(plan);
        assertThat(json).contains(ORDINAL);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    void testRoundTrip()
    {
        assertThat(codec.fromJson(json)).isEqualTo(plan);
    }

    @Test
    void testMissingPrimitiveIsRejected()
    {
        assertThatThrownBy(() -> codec.fromJson(json.replace("," + ORDINAL, "")))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Cannot map `null` into type `int`");
    }

    @Test
    void testNullPrimitiveIsRejected()
    {
        assertThatThrownBy(() -> codec.fromJson(json.replace(ORDINAL, "\"ordinal\":null")))
                .isInstanceOf(IllegalArgumentException.class)
                .cause()
                .hasMessageContaining("Cannot map `null` into type `int`");
    }

    public record OrdinalColumnId(int ordinal)
            implements ConnectorColumnId
    {
        private static final ConnectorIdVersion VERSION = new ConnectorIdVersion("OrdinalColumnId", 1);

        @Override
        public ConnectorIdVersion version()
        {
            return VERSION;
        }
    }
}
