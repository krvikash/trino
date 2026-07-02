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

import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.Duration;
import io.starburst.ai.model.ConnectionInfo.OAuth2GrantType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOAuth2TokenCache
{
    private static final ResolvedOAuth2Config CONFIG_A = new ResolvedOAuth2Config(
            OAuth2GrantType.CLIENT_CREDENTIALS,
            "https://idp.example/token",
            "client-a",
            "secret-a",
            Optional.empty(),
            Optional.empty());

    private static final ResolvedOAuth2Config CONFIG_B = new ResolvedOAuth2Config(
            OAuth2GrantType.CLIENT_CREDENTIALS,
            "https://idp.example/token",
            "client-b",
            "secret-b",
            Optional.empty(),
            Optional.empty());

    @Test
    public void cachesTokenUntilExpiry()
    {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        AtomicInteger fetches = new AtomicInteger();
        OAuth2TokenFetcher fetcher = new TestFetcher(_ -> {
            fetches.incrementAndGet();
            return new OAuth2TokenResponse("token-" + fetches.get(), 300L);
        });
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("token-1");
        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("token-1");
        assertThat(fetches.get()).isEqualTo(1);

        // Effective expiry = now + 300 - 60 = now + 240s. Advance to 239s -> still valid.
        clock.advanceSeconds(239);
        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("token-1");
        assertThat(fetches.get()).isEqualTo(1);

        // Cross expiry boundary -> refetch.
        clock.advanceSeconds(2);
        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("token-2");
        assertThat(fetches.get()).isEqualTo(2);
    }

    @Test
    public void failureIsFailClosed()
    {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        AtomicInteger fetches = new AtomicInteger();
        AtomicReference<Boolean> shouldFail = new AtomicReference<>(true);
        OAuth2TokenFetcher fetcher = new TestFetcher(_ -> {
            fetches.incrementAndGet();
            if (shouldFail.get()) {
                throw new OAuth2TokenException("simulated IdP failure");
            }
            return new OAuth2TokenResponse("recovered", 600L);
        });
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        assertThatThrownBy(() -> cache.accessToken("model-a", CONFIG_A))
                .isInstanceOf(OAuth2TokenException.class)
                .hasMessage("simulated IdP failure");
        assertThat(fetches.get()).isEqualTo(1);
        assertThat(cache.isExpired("model-a", CONFIG_A)).isTrue();

        // Subsequent call retries — cache did not retain a stale entry.
        shouldFail.set(false);
        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("recovered");
        assertThat(fetches.get()).isEqualTo(2);
    }

    @Test
    public void retainKeysEvictsOthers()
    {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        OAuth2TokenFetcher fetcher = new TestFetcher(config -> new OAuth2TokenResponse("tok-" + config.clientId(), 600L));
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        cache.accessToken("model-a", CONFIG_A);
        cache.accessToken("model-b", CONFIG_B);
        assertThat(cache.isExpired("model-a", CONFIG_A)).isFalse();
        assertThat(cache.isExpired("model-b", CONFIG_B)).isFalse();

        cache.retainKeys(Map.of("model-a", CONFIG_A));
        assertThat(cache.isExpired("model-a", CONFIG_A)).isFalse();
        assertThat(cache.isExpired("model-b", CONFIG_B)).isTrue();
    }

    @Test
    public void accessTokenAfterRetainKeysEvictionRefetches()
    {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        AtomicInteger fetches = new AtomicInteger();
        OAuth2TokenFetcher fetcher = new TestFetcher(_ -> new OAuth2TokenResponse("tok-" + fetches.incrementAndGet(), 600L));
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("tok-1");
        cache.retainKeys(Map.of());
        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("tok-2");
        assertThat(fetches.get()).isEqualTo(2);
    }

    @Test
    public void throwsWhenFetchedTokenIsAlreadyExpired()
    {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        AtomicInteger fetches = new AtomicInteger();
        // expires_in (1s) minus refreshSkew (60s) yields an effective expiry 59s in the past.
        OAuth2TokenFetcher fetcher = new TestFetcher(_ -> {
            fetches.incrementAndGet();
            return new OAuth2TokenResponse("stale", 1L);
        });
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        assertThatThrownBy(() -> cache.accessToken("model-a", CONFIG_A))
                .isInstanceOf(OAuth2TokenException.class)
                .hasMessage("OAuth2 token was already expired immediately after fetch");
        // Safety-net path invalidates once and re-fetches once before giving up.
        assertThat(fetches.get()).isEqualTo(2);
        assertThat(cache.isExpired("model-a", CONFIG_A)).isTrue();
    }

    @Test
    public void maximumSizeBoundsCache()
    {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        OAuth2TokenFetcher fetcher = new TestFetcher(config -> new OAuth2TokenResponse("tok-" + config.clientId(), 3600L));
        // maximumSize=2 with three distinct configs must evict at least one of the first two.
        // Guava's size-based eviction is approximate LRU, so we only assert size <= 2 (not which key).
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(10, TimeUnit.MINUTES), 2, clock);

        ResolvedOAuth2Config configC = new ResolvedOAuth2Config(
                OAuth2GrantType.CLIENT_CREDENTIALS,
                "https://idp.example/token",
                "client-c",
                "secret-c",
                Optional.empty(),
                Optional.empty());

        cache.accessToken("model-a", CONFIG_A);
        cache.accessToken("model-b", CONFIG_B);
        cache.accessToken("model-c", configC);

        int stillPresent = 0;
        if (!cache.isExpired("model-a", CONFIG_A)) {
            stillPresent++;
        }
        if (!cache.isExpired("model-b", CONFIG_B)) {
            stillPresent++;
        }
        if (!cache.isExpired("model-c", configC)) {
            stillPresent++;
        }
        assertThat(stillPresent).isLessThanOrEqualTo(2);
        // The most recently written entry should always survive since it hasn't been targeted for eviction.
        assertThat(cache.isExpired("model-c", configC)).isFalse();
    }

    @Test
    public void separateModelIdsAreIndependentlyTracked()
    {
        // model-a and model-b share the identical OAuth2 config. The composite (modelId, config) key
        // must produce independent cache entries so that evicting one does not affect the other.
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        AtomicInteger fetches = new AtomicInteger();
        OAuth2TokenFetcher fetcher = new TestFetcher(_ -> new OAuth2TokenResponse("tok-" + fetches.incrementAndGet(), 600L));
        OAuth2TokenCache cache = new OAuth2TokenCache(fetcher, new Duration(60, SECONDS), new Duration(60, SECONDS), 1000, clock);

        assertThat(cache.accessToken("model-a", CONFIG_A)).isEqualTo("tok-1");
        assertThat(cache.accessToken("model-b", CONFIG_A)).isEqualTo("tok-2");
        assertThat(fetches.get()).isEqualTo(2); // separate entries — two fetches

        // Evict model-a only.
        cache.retainKeys(Map.of("model-b", CONFIG_A));

        assertThat(cache.isExpired("model-a", CONFIG_A)).isTrue();
        assertThat(cache.isExpired("model-b", CONFIG_A)).isFalse();
        // model-b's cached token survives — no additional fetch.
        assertThat(cache.accessToken("model-b", CONFIG_A)).isEqualTo("tok-2");
        assertThat(fetches.get()).isEqualTo(2);
    }

    private interface FetchAction
    {
        OAuth2TokenResponse apply(ResolvedOAuth2Config config);
    }

    private static final class TestFetcher
            extends OAuth2TokenFetcher
    {
        private final FetchAction action;

        private TestFetcher(FetchAction action)
        {
            super(new TestingHttpClient(_ -> {
                throw new UnsupportedOperationException("HttpClient should not be invoked by TestFetcher");
            }));
            this.action = action;
        }

        @Override
        public OAuth2TokenResponse fetch(ResolvedOAuth2Config config)
        {
            return action.apply(config);
        }
    }

    private static final class MutableClock
            extends Clock
    {
        private Instant now;

        private MutableClock(Instant start)
        {
            this.now = start;
        }

        void advanceSeconds(long seconds)
        {
            now = now.plusSeconds(seconds);
        }

        @Override
        public ZoneOffset getZone()
        {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant()
        {
            return now;
        }
    }
}
