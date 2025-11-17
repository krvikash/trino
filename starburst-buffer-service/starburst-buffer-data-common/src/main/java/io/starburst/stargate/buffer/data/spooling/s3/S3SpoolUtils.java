/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.starburst.stargate.buffer.data.spooling.azure.AzureSpoolUtils.PATH_SEPARATOR;
import static java.util.Objects.requireNonNull;

public final class S3SpoolUtils
{
    private S3SpoolUtils() {}

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket cannot be determined from the URI.
     */
    public static S3UriInfo getS3UriInfo(URI uri)
    {
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
        }

        String bucketName;
        if (uri.getHost() != null) {
            bucketName = uri.getHost();
        }
        else if (uri.getUserInfo() == null) {
            bucketName = uri.getAuthority();
        }
        else {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
        }

        if (bucketName.contains("@") || bucketName.contains(":")) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
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

        return new S3UriInfo(bucketName, path);
    }

    public static String keyFromUri(URI uri)
    {
        checkArgument(uri.isAbsolute(), "Uri is not absolute: %s", uri);
        String key = nullToEmpty(uri.getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    public record S3UriInfo(String bucket, String path)
    {
        public S3UriInfo
        {
            requireNonNull(bucket, "bucket is null");
            requireNonNull(path, "path is null");
        }
    }
}
