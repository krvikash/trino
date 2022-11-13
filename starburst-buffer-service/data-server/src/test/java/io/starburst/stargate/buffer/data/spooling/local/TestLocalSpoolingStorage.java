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

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.buffer.data.spooling.AbstractTestSpoolingStorage;
import io.starburst.stargate.buffer.data.spooling.SpoolingStorage;

public class TestLocalSpoolingStorage
        extends AbstractTestSpoolingStorage
{
    @Override
    protected SpoolingStorage createSpoolingStorage()
    {
        return createSpoolingStorage(ImmutableMap.<String, String>builder()
                .put("discovery-service.uri", "http://dummy") // needed for bootstrap
                .put("spooling.directory", System.getProperty("java.io.tmpdir") + "/spooling-storage")
                .build());
    }
}
