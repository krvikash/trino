/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.authentication;

import io.airlift.http.client.Request;

public class NoneAuthenticator
        implements OpenApiAuthenticator
{
    @Override
    public Request filterRequest(Request request)
    {
        return request;
    }
}
