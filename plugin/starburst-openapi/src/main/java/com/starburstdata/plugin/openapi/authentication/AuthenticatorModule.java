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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.binder.LinkedBindingBuilder;
import com.starburstdata.plugin.openapi.ForOpenApi;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;

import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class AuthenticatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        LinkedBindingBuilder<OpenApiAuthenticator> authenticatorBindingBuilder = binder.bind(OpenApiAuthenticator.class);
        (switch (buildConfigObject(AuthenticatorTypeConfig.class).getType()) {
            case NONE -> authenticatorBindingBuilder.to(NoneAuthenticator.class);
            case APIKEY -> {
                configBinder(binder).bindConfig(ApiKeyAuthenticatorConfig.class);
                yield authenticatorBindingBuilder.toProvider(ApiKeyAuthenticatorProvider.class);
            }
            case OAUTH2 -> {
                configBinder(binder).bindConfig(Oauth2AuthenticatorConfig.class);
                yield authenticatorBindingBuilder.toProvider(Oauth2AuthenticatorProvider.class);
            }
        }).in(SINGLETON);
    }

    public static class ApiKeyAuthenticatorProvider
            implements Provider<ApiKeyAuthenticator>
    {
        private final ApiKeyAuthenticatorConfig apiKeyAuthenticatorConfig;

        @Inject
        public ApiKeyAuthenticatorProvider(ApiKeyAuthenticatorConfig apiKeyAuthenticatorConfig)
        {
            this.apiKeyAuthenticatorConfig = requireNonNull(apiKeyAuthenticatorConfig, "apiKeyAuthenticatorConfig is null");
        }

        @Override
        public ApiKeyAuthenticator get()
        {
            return new ApiKeyAuthenticator(
                    apiKeyAuthenticatorConfig.getSecret(),
                    apiKeyAuthenticatorConfig.getIn(),
                    apiKeyAuthenticatorConfig.getName());
        }
    }

    public static class Oauth2AuthenticatorProvider
            implements Provider<Oauth2Authenticator>
    {
        private final HttpClient httpClient;
        private final ObjectMapper objectMapper;
        private final Oauth2AuthenticatorConfig oauth2AuthenticatorConfig;

        @Inject
        public Oauth2AuthenticatorProvider(
                Oauth2AuthenticatorConfig oauth2AuthenticatorConfig,
                @ForOpenApi HttpClient httpClient,
                ObjectMapper objectMapper)
        {
            this.oauth2AuthenticatorConfig = requireNonNull(oauth2AuthenticatorConfig, "oauth2AuthenticatorConfig is null");
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        }

        @Override
        public Oauth2Authenticator get()
        {
            return new Oauth2Authenticator(
                    oauth2AuthenticatorConfig.getTokenUrl(),
                    Optional.ofNullable(oauth2AuthenticatorConfig.getScopes()),
                    oauth2AuthenticatorConfig.getClientId(),
                    oauth2AuthenticatorConfig.getClientSecret(),
                    httpClient,
                    objectMapper);
        }
    }
}
