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

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

interface URLRequester
{
    Duration INFINITE_DURATION = Duration.ZERO;
    Duration MINIMUM_DURATION = Duration.ofMillis(1);

    default byte[] get(URL url, Duration connectTimeout)
            throws IOException
    {
        return get(url, connectTimeout, Collections.emptyMap());
    }

    byte[] get(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException;

    byte[] put(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException;
}
