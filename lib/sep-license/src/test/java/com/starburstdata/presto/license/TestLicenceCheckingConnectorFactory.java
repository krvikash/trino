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

import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestLicenceCheckingConnectorFactory
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorFactory.class, LicenceCheckingConnectorFactory.class);
    }
}
