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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.starburst.ai.client.AiClientConfig;
import io.trino.cache.EvictableCacheBuilder;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2TokenCache
{
    private final Cache<Oauth2TokenKey, Token> cache;
    private final OAuth2TokenFetcher fetcher;
    private final Clock clock;
    private final Duration refreshSkew;

    @Inject
    public OAuth2TokenCache(OAuth2TokenFetcher fetcher, AiClientConfig config)
    {
        this(fetcher, config.getOauth2RefreshSkew(), config.getOauthTokenCacheDuration(), config.getOauth2MaxCachedTokens(), Clock.systemUTC());
    }

    @VisibleForTesting
    OAuth2TokenCache(OAuth2TokenFetcher fetcher, Duration refreshSkew, Duration expireAfterWrite, int maxCachedTokens, Clock clock)
    {
        this.fetcher = requireNonNull(fetcher, "fetcher is null");
        this.refreshSkew = requireNonNull(refreshSkew, "refreshSkew is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.cache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(expireAfterWrite.toJavaTime())
                .maximumSize(maxCachedTokens)
                .build();
    }

    private Token loadOrFetch(String id, ResolvedOAuth2Config config)
    {
        Oauth2TokenKey tokenKey = new Oauth2TokenKey(id, config);
        try {
            return cache.get(tokenKey, () -> {
                OAuth2TokenResponse response = fetcher.fetch(config);
                checkArgument(response.accessToken() != null && !response.accessToken().isBlank(), "access_token was blank");
                Instant expiresAt = clock.instant().plusSeconds(response.expiresInSeconds()).minus(refreshSkew.toJavaTime());
                return new Token(response.accessToken(), expiresAt);
            });
        }
        catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof OAuth2TokenException oauth) {
                throw oauth;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new OAuth2TokenException("OAuth2 token fetch failed", cause);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof OAuth2TokenException oauth) {
                throw oauth;
            }
            throw new OAuth2TokenException("OAuth2 token fetch failed", cause);
        }
    }

    public String accessToken(String modelId, ResolvedOAuth2Config config)
    {
        Oauth2TokenKey tokenKey = new Oauth2TokenKey(modelId, config);
        Token token = loadOrFetch(modelId, config);
        if (isExpired(token)) {
            // Token's IdP-declared lifetime has elapsed while the entry was still in the Guava cache.
            // Evict and force a fresh fetch. If the very next fetch also comes back already-expired
            // (extremely short expires_in with a large skew), fail rather than loop forever.
            cache.invalidate(tokenKey);
            token = loadOrFetch(modelId, config);
            if (isExpired(token)) {
                throw new OAuth2TokenException("OAuth2 token was already expired immediately after fetch");
            }
        }
        return token.accessToken();
    }

    /**
     * Returns true when there is no cached token for the given configuration, or when the cached token
     * has passed its effective (skew-adjusted) expiry. Used by the reloading client provider to decide
     * whether the OpenAI client for a spec must be rebuilt with a fresh bearer.
     */
    public boolean isExpired(String modelId, ResolvedOAuth2Config config)
    {
        Oauth2TokenKey tokenKey = new Oauth2TokenKey(modelId, config);
        Token token = cache.getIfPresent(tokenKey);
        return token == null || isExpired(token);
    }

    /**
     * Evicts cache entries whose keys are not in the supplied set. Called by the reloading provider
     * at the end of each successful reload so tokens for removed specs are dropped immediately rather
     * than waiting for the 30-minute lifetime ceiling.
     */
    public void retainKeys(Map<String, ResolvedOAuth2Config> currentKeys)
    {
        Set<Oauth2TokenKey> retained = currentKeys.entrySet().stream()
                .map(entry -> new Oauth2TokenKey(entry.getKey(), entry.getValue()))
                .collect(toImmutableSet());
        cache.asMap().keySet().removeIf(key -> !retained.contains(key));
    }

    record Oauth2TokenKey(String id, ResolvedOAuth2Config oauth2Config)
    {
        Oauth2TokenKey
        {
            requireNonNull(id, "id is null");
            requireNonNull(oauth2Config, "oauth2Config is null");
        }
    }

    private boolean isExpired(Token token)
    {
        return !clock.instant().isBefore(token.expiresAt());
    }

    private record Token(String accessToken, Instant expiresAt)
    {
        private Token
        {
            requireNonNull(accessToken, "accessToken is null");
            requireNonNull(expiresAt, "expiresAt is null");
        }

        @Override
        public String toString()
        {
            return format("Token{accessToken=***, expiresAt=%s}", expiresAt);
        }
    }
}
