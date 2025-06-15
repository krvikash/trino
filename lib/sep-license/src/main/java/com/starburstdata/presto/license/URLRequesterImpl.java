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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class URLRequesterImpl
        implements URLRequester
{
    @Override
    public byte[] get(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException
    {
        return doRequest(
                requireNonNull(url, "url is null"),
                requireNonNull(connectTimeout, "connectTimeout is null"),
                requireNonNull(headers, "headers is null"),
                url.getProtocol().startsWith("http") ? "GET" : null);
    }

    @Override
    public byte[] put(URL url, Duration connectTimeout, Map<String, String> headers)
            throws IOException
    {
        return doRequest(
                requireNonNull(url, "url is null"),
                requireNonNull(connectTimeout, "connectTimeout is null"),
                requireNonNull(headers, "headers is null"),
                url.getProtocol().startsWith("http") ? "PUT" : null);
    }

    private byte[] doRequest(URL url, Duration connectTimeout, Map<String, String> headers, String httpMethod)
            throws IOException
    {
        URLConnection connection = url.openConnection();
        connection.setConnectTimeout((int) connectTimeout.toMillis());
        connection.setReadTimeout((int) connectTimeout.toMillis());
        connection.setDoOutput(false);
        headers.forEach(connection::addRequestProperty);

        if (httpMethod != null) {
            if (connection instanceof HttpURLConnection httpURLConnection) {
                httpURLConnection.setRequestMethod(httpMethod);
            }
            else {
                throw new IllegalArgumentException(format("Http method %s given, but URL %s use different protocol", httpMethod, url));
            }
        }

        connection.connect();

        try (InputStream inputStream = connection.getInputStream()) {
            return inputStream.readAllBytes();
        }
    }
}
