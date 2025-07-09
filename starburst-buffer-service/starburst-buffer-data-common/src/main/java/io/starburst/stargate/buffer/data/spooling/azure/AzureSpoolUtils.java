/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.azure;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public class AzureSpoolUtils
{
    public static final String PATH_SEPARATOR = "/";

    public static String getHostName(URI uri)
    {
        return requireNonNull(uri.getHost(), "Invalid abfs URI passed to getHostName: " + uri);
    }

    public static String getContainerName(URI uri)
    {
        return requireNonNull(uri.getUserInfo(), "Invalid abfs URI passed to getContainerName: " + uri);
    }

    public static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        checkArgument(key.length() > 0, "Invalid abfs URI passed to keyFromUri: %s", uri);
        return key;
    }

    private AzureSpoolUtils() {}
}
