/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

import com.google.common.net.MediaType;

public final class TrinoMediaTypes
{
    public static final String TRINO_PAGES = "application/X-trino-pages";
    public static final MediaType TRINO_PAGES_TYPE = MediaType.create("application", "X-trino-pages");
    public static final String TRINO_CHUNK_DATA = "application/X-trino-buffer-chunk-data";
    public static final MediaType TRINO_CHUNK_DATA_TYPE = MediaType.create("application", "X-trino-buffer-chunk-data");

    private TrinoMediaTypes() {}
}
