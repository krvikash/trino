/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import io.trino.spi.TrinoException;

import java.net.URI;
import java.util.Locale;

import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSAFE_REQUEST_URI;
import static java.util.Objects.requireNonNull;

/**
 * The single authority the connector may send authenticated requests to.
 */
public record AllowedAuthority(String scheme, String host, int port)
{
    private static final int HTTP_DEFAULT_PORT = 80;
    private static final int HTTPS_DEFAULT_PORT = 443;

    public AllowedAuthority
    {
        requireNonNull(scheme, "scheme is null");
        requireNonNull(host, "host is null");
    }

    public static AllowedAuthority forBaseUri(URI baseUri)
    {
        requireNonNull(baseUri, "baseUri is null");
        String scheme = normalizeScheme(baseUri);
        String host = normalizeHost(baseUri);
        if (scheme == null || host == null) {
            throw new TrinoException(
                    OPENAPI_UNSAFE_REQUEST_URI,
                    "openapi.base-uri must be an absolute http or https URI with a host");
        }
        return new AllowedAuthority(scheme, host, defaultedPort(scheme, baseUri.getPort()));
    }

    /**
     * @return the URI, unchanged, if it targets this authority
     * @throws TrinoException if it does not
     */
    public URI check(URI uri)
    {
        requireNonNull(uri, "uri is null");
        String scheme = normalizeScheme(uri);
        String host = normalizeHost(uri);
        int port = defaultedPort(scheme, uri.getPort());
        if (scheme == null || host == null || !isAllowed(scheme, host, port)) {
            throw new TrinoException(
                    OPENAPI_UNSAFE_REQUEST_URI,
                    "Refusing to request %s: only %s is permitted, which is the authority of openapi.base-uri"
                            .formatted(sanitize(scheme, host, port), sanitize(this.scheme, this.host, this.port)));
        }
        if (uri.getRawUserInfo() != null) {
            throw new TrinoException(
                    OPENAPI_UNSAFE_REQUEST_URI,
                    "Refusing to request %s: the URI embeds credentials".formatted(sanitize(scheme, host, port)));
        }
        return uri;
    }

    private boolean isAllowed(String scheme, String host, int port)
    {
        return host.equals(this.host) && (
                (scheme.equals(this.scheme) && port == this.port) || (
                // An http authority may redirect to https on the same host. The reverse is a downgrade and stays rejected.
                        scheme.equals("https") && this.scheme.equals("http") && (port == this.port || (port == HTTPS_DEFAULT_PORT && this.port == HTTP_DEFAULT_PORT))));
    }

    private static String sanitize(String scheme, String host, int port)
    {
        return port != -1
                ? "%s://%s:%s".formatted(scheme, host, port)
                : "%s://%s".formatted(scheme, host);
    }

    private static String normalizeScheme(URI uri)
    {
        if (!uri.isAbsolute() || uri.getScheme() == null) {
            return null;
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        return scheme.equals("http") || scheme.equals("https") ? scheme : null;
    }

    private static String normalizeHost(URI uri)
    {
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            return null;
        }
        return host.toLowerCase(Locale.ROOT);
    }

    private static int defaultedPort(String scheme, int port)
    {
        return scheme == null || port != -1
                ? port
                : scheme.equals("https") ? HTTPS_DEFAULT_PORT : HTTP_DEFAULT_PORT;
    }
}
