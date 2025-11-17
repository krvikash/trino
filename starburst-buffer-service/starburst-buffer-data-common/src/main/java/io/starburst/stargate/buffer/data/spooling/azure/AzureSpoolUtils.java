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

public final class AzureSpoolUtils
{
    public static final String PATH_SEPARATOR = "/";

    private AzureSpoolUtils() {}

    public static AzureUriInfo getAzureUriInfo(URI uri)
    {
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid abfs URI: " + uri);
        }
        String containerName = uri.getUserInfo();
        if (containerName == null) {
            throw new IllegalArgumentException("Invalid abfs URI: " + uri);
        }
        String path = uri.getPath();
        if (path == null) {
            path = "";
        }

        while (path.startsWith(PATH_SEPARATOR)) {
            path = path.substring(1);
        }
        while (path.endsWith(PATH_SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }

        return new AzureUriInfo(host, containerName, path);
    }

    public static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        checkArgument(!key.isEmpty(), "Invalid abfs URI passed to keyFromUri: %s", uri);
        return key;
    }

    public record AzureUriInfo(String hostName, String containerName, String path)
    {
        public AzureUriInfo
        {
            requireNonNull(hostName, "hostName is null");
            requireNonNull(containerName, "containerName is null");
            requireNonNull(path, "path is null");
        }
    }
}
