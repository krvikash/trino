/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.openai.oauth;

import io.starburst.stargate.security.OAuthServerUrlValidator;
import io.starburst.stargate.security.RejectedUrlException;

import java.net.URI;

import static io.starburst.stargate.security.RejectedUrlException.Reason.HOST;
import static io.starburst.stargate.security.RejectedUrlException.Reason.SCHEME;
import static io.starburst.stargate.security.SsrfEgressValidator.validateEgress;
import static java.util.Objects.requireNonNull;

// TODO: replace with a stargate OAuthServerUrlValidator method that lets the caller specify the allowed schemes
public class HttpAllowingOAuthServerUrlValidator
        implements OAuthServerUrlValidator
{
    @Override
    public void validateOAuthEndpointUrl(URI url)
            throws RejectedUrlException
    {
        requireNonNull(url, "url is null");
        if (!"https".equalsIgnoreCase(url.getScheme()) && !"http".equalsIgnoreCase(url.getScheme())) {
            throw new RejectedUrlException(SCHEME, "Only HTTP and HTTPS URLs are allowed.");
        }
        String host = url.getHost();
        if (host == null || host.equals("localhost")) {
            throw new RejectedUrlException(HOST, "URL must have a valid host.");
        }
        validateEgress(url);
    }
}
