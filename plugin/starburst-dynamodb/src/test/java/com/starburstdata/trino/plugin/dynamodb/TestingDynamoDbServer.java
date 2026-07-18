/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class TestingDynamoDbServer
        implements AutoCloseable
{
    private static final int PORT = 8000;

    private final GenericContainer<?> dockerContainer;

    public TestingDynamoDbServer()
    {
        dockerContainer = new GenericContainer<>("amazon/dynamodb-local:2.5.4")
                .withExposedPorts(PORT)
                .waitingFor(Wait.forLogMessage(".*Initializing DynamoDB Local with the following configuration.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dockerContainer.start();
    }

    public String getEndpointUrl()
    {
        return "http://localhost:" + dockerContainer.getMappedPort(PORT);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
