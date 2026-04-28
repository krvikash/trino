/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.node.NodeInfo;
import io.trino.plugin.warp.server.security.SecurityConfig;

import javax.crypto.KDF;
import javax.crypto.SecretKey;
import javax.crypto.spec.HKDFParameterSpec;

import java.security.spec.AlgorithmParameterSpec;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.trino.plugin.warp.server.security.jwt.JwtUtil.newJwtBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

// Mirrored from io.trino.server.InternalAuthenticationManager in trino-main
// Only the client-side (filterRequest) functionality is included.
// Server-side methods (handleInternalRequest, isInternalRequest) are omitted.
public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static Instant defaultExpirationSupplier()
    {
        return Instant.now().plus(6, MINUTES);
    }

    // Leave a 5 minute buffer to allow for clock skew and GC pauses
    private static Instant tokenReuseThreshold(Instant instant)
    {
        return instant.minus(5, MINUTES);
    }

    private static final HeaderName TRINO_INTERNAL_BEARER = HeaderName.of("X-Trino-Internal-Bearer");

    private final SecretKey hmac;
    private final String nodeId;
    private final AtomicReference<InternalToken> currentToken;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), nodeInfo.getNodeId());
    }

    private static String getSharedSecret(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo, boolean authenticationEnabled)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");

        // This check should not be required (as bean validation already checked it),
        // but be extra careful to not use a known secret for authentication.
        if (!internalCommunicationConfig.isRequiredSharedSecretSet()) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when internal communications uses HTTPS");
        }

        if (internalCommunicationConfig.getSharedSecret().isEmpty() && authenticationEnabled) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when authentication is enabled");
        }

        return internalCommunicationConfig.getSharedSecret().orElseGet(nodeInfo::getEnvironment);
    }

    public InternalAuthenticationManager(String sharedSecret, String nodeId)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.hmac = expandKey(sharedSecret);
        this.nodeId = nodeId;
        this.currentToken = new AtomicReference<>(createJwt());
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(TRINO_INTERNAL_BEARER, getOrGenerateJwt())
                .build();
    }

    private String getOrGenerateJwt()
    {
        InternalToken token = currentToken.get();
        if (token.isExpired()) {
            InternalToken newToken = createJwt();
            if (currentToken.compareAndSet(token, newToken)) {
                token = newToken;
            }
            else {
                // Another thread already generated a new token
                token = currentToken.get();
            }
        }
        return token.token();
    }

    private InternalToken createJwt()
    {
        Instant expiration = defaultExpirationSupplier();
        return new InternalToken(expiration, newJwtBuilder()
                .signWith(hmac)
                .subject(nodeId)
                .expiration(Date.from(expiration))
                .compact());
    }

    private record InternalToken(Instant expiration, String token)
    {
        InternalToken
        {
            expiration = tokenReuseThreshold(requireNonNull(expiration, "expiration is null"));
            requireNonNull(token, "token is null");
        }

        boolean isExpired()
        {
            return Instant.now().isAfter(expiration);
        }
    }

    private static SecretKey expandKey(String sharedSecret)
    {
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");

            AlgorithmParameterSpec params =
                    HKDFParameterSpec.ofExtract()
                            .addIKM(sharedSecret.getBytes(UTF_8))
                            .thenExpand("internal-communication".getBytes(UTF_8), 32);

            return hkdf.deriveKey("HmacSHA256", params);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not expand internal communication shared key using HKDF-SHA256", e);
        }
    }
}
