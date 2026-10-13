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

import io.airlift.http.client.Request;
import io.starburst.stargate.security.RejectedUrlException;
import io.starburst.stargate.security.SsrfEgressOAuthServerUrlValidator;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOAuthEndpointSsrfFilter
{
    private final OAuthEndpointSsrfFilter filter = new OAuthEndpointSsrfFilter(new SsrfEgressOAuthServerUrlValidator());
    private final OAuthEndpointSsrfFilter httpAllowedFilter = new OAuthEndpointSsrfFilter(new HttpAllowingOAuthServerUrlValidator());

    @Test
    void rejectsInternalLiteralHost()
    {
        // Literal IPs are checked without DNS, so these do not touch the network.
        assertThatThrownBy(() -> filter.filterRequest(request("https://127.0.0.1/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URL is invalid or blocked.")
                .hasCauseInstanceOf(RejectedUrlException.class);
        assertThatThrownBy(() -> filter.filterRequest(request("https://169.254.169.254/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URL is invalid or blocked.")
                .hasCauseInstanceOf(RejectedUrlException.class);
    }

    @Test
    void rejectsNonHttps()
    {
        assertThatThrownBy(() -> filter.filterRequest(request("http://8.8.8.8/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only HTTPS URLs are allowed.")
                .hasCauseInstanceOf(RejectedUrlException.class);
    }

    @Test
    void allowsHttpWhenConfigured()
    {
        assertThatCode(() -> httpAllowedFilter.filterRequest(request("http://8.8.8.8/token")))
                .doesNotThrowAnyException();
        assertThatCode(() -> httpAllowedFilter.filterRequest(request("http://8.8.8.8:8080/token")))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsInternalHostWhenHttpAllowed()
    {
        assertThatThrownBy(() -> httpAllowedFilter.filterRequest(request("http://169.254.169.254/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URL is invalid or blocked.")
                .hasCauseInstanceOf(RejectedUrlException.class);
        assertThatThrownBy(() -> httpAllowedFilter.filterRequest(request("https://10.0.0.1/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URL is invalid or blocked.")
                .hasCauseInstanceOf(RejectedUrlException.class);
        assertThatThrownBy(() -> httpAllowedFilter.filterRequest(request("http://localhost/token")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URL must have a valid host.")
                .hasCauseInstanceOf(RejectedUrlException.class);
    }

    @Test
    void allowsPublicLiteralHost()
    {
        assertThatCode(() -> filter.filterRequest(request("https://8.8.8.8/token")))
                .doesNotThrowAnyException();
    }

    private static Request request(String uri)
    {
        return prepareGet().setUri(URI.create(uri)).build();
    }
}
