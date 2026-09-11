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

import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;

import static io.airlift.http.client.Request.Builder.fromRequest;

public class DisableRedirectsFilter
        implements HttpRequestFilter
{
    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .setFollowRedirects(false)
                .build();
    }
}
