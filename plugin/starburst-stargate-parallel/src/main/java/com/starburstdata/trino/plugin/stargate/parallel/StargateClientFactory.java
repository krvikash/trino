/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.inject.Inject;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.client.uri.TrinoUri;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.credential.CredentialConfig;

import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.TrinoUriFactory.authProperties;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static java.util.Objects.requireNonNull;

public class StargateClientFactory
{
    private static final String SOURCE = "stargate-parallel";

    private final String encoding;
    private final TrinoUri trinoUri;

    @Inject
    public StargateClientFactory(StargateParallelConfig parallelConfig, BaseJdbcConfig config, CredentialConfig credentialConfig)
    {
        this.encoding = requireNonNull(parallelConfig, "parallelConfig is null").getEncoding();
        this.trinoUri = TrinoUri.create(config.getConnectionUrl().replaceFirst("jdbc:", ""), authProperties(credentialConfig));
    }

    public StatementClient createFactory(String query)
    {
        ClientSession session = trinoUri.toClientSessionBuilder()
                .source(SOURCE)
                .encoding(Optional.of(encoding))
                .build();
        return newStatementClient(trinoUri, SOURCE, session, query, Optional.empty());
    }
}
