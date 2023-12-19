/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.data.client.DataApi;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;

public interface ApiFactory
{
    DiscoveryApi createDiscoveryApi();

    DataApi createDataApi(BufferNodeInfo nodeInfo);
}
