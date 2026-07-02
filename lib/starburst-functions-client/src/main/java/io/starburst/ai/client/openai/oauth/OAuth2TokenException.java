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

public class OAuth2TokenException
        extends RuntimeException
{
    public OAuth2TokenException(String message)
    {
        super(message);
    }

    public OAuth2TokenException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
