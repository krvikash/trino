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

import io.trino.spi.eventlistener.EventListenerFactory;
import org.junit.jupiter.api.Test;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestLicenceCheckingEventListenerFactory
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(EventListenerFactory.class, LicenceCheckingEventListenerFactory.class);
    }
}
