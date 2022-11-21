/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.local;

import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

import static io.starburst.stargate.buffer.data.spooling.SpoolTestHelper.createLocalSpoolingStorage;

public class TestLocalSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createLocalSpoolingStorage();
    }
}
