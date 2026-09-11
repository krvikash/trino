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
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.airlift.http.client.Request.Builder.preparePost;
import static org.assertj.core.api.Assertions.assertThat;

class TestDisableRedirectsFilter
{
    @Test
    void disablesRedirects()
    {
        Request request = preparePost()
                .setUri(URI.create("https://idp.example/token"))
                .setFollowRedirects(true)
                .build();

        Request filtered = new DisableRedirectsFilter().filterRequest(request);

        assertThat(filtered.isFollowRedirects()).isFalse();
        assertThat(filtered.getUri()).isEqualTo(request.getUri());
        assertThat(filtered.getMethod()).isEqualTo("POST");
    }

    @Test
    void disablesDefaultRedirects()
    {
        Request request = preparePost()
                .setUri(URI.create("https://idp.example/token"))
                .build();

        Request filtered = new DisableRedirectsFilter().filterRequest(request);

        assertThat(filtered.isFollowRedirects()).isFalse();
    }
}
