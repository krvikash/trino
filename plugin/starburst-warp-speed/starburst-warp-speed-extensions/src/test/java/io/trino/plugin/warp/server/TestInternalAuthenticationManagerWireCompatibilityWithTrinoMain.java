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
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.jsonwebtoken.JwtParser;
import io.trino.server.StartupStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the stripped-down warp {@link InternalAuthenticationManager} produces tokens
 * that the full trino-main {@link io.trino.server.InternalAuthenticationManager} accepts.
 * This is the actual contract that matters for cross-service auth between warp clients and
 * Trino servers.
 * <p>
 * Failure modes this test catches:
 * <ul>
 *   <li>Drift in HKDF key derivation (warp and trino-main {@code expandKey} methods diverge).</li>
 *   <li>Drift in the JWT signing recipe (signer, algorithm, claims).</li>
 *   <li>Drift in the bearer header name ({@code X-Trino-Internal-Bearer}).</li>
 * </ul>
 */
public class TestInternalAuthenticationManagerWireCompatibilityWithTrinoMain
{
    private static final String SHARED_SECRET = "very-secret-shared-key-for-testing";
    private static final String NODE_ID = "test-node-id";

    @Test
    public void testWarpBearerVerifiesAgainstTrinoMainParser()
            throws Exception
    {
        InternalAuthenticationManager warpManager = new InternalAuthenticationManager(SHARED_SECRET, NODE_ID);
        io.trino.server.InternalAuthenticationManager trinoMainManager =
                new io.trino.server.InternalAuthenticationManager(SHARED_SECRET, NODE_ID, new StartupStatus());

        Request baseRequest = prepareGet().setUri(URI.create("http://example/")).build();
        Request signedByWarp = warpManager.filterRequest(baseRequest);
        Request signedByTrinoMain = trinoMainManager.filterRequest(baseRequest);

        // Both filters must add the same header name; otherwise a Trino server reading what
        // warp wrote would not find it.
        List<HeaderName> warpAddedHeaders = ImmutableList.copyOf(signedByWarp.getHeaders().keySet());
        List<HeaderName> trinoMainAddedHeaders = ImmutableList.copyOf(signedByTrinoMain.getHeaders().keySet());
        assertThat(warpAddedHeaders).isEqualTo(trinoMainAddedHeaders);
        assertThat(warpAddedHeaders).hasSize(1);
        HeaderName bearerHeader = warpAddedHeaders.get(0);

        String warpBearer = signedByWarp.getHeader(bearerHeader);
        assertThat(warpBearer).isNotNull();

        // Use the production parser stored in trino-main's manager — the exact code path a
        // Trino server takes when authenticating an incoming warp request. Reflection here is
        // intentional: it pins the test to the same parser instance the server uses, so any
        // change to trino-main's verification recipe surfaces here.
        JwtParser productionParser = extractJwtParser(trinoMainManager);

        String subject = productionParser.parseSignedClaims(warpBearer).getPayload().getSubject();
        assertThat(subject).isEqualTo(NODE_ID);
    }

    private static JwtParser extractJwtParser(io.trino.server.InternalAuthenticationManager manager)
            throws ReflectiveOperationException
    {
        Field field = io.trino.server.InternalAuthenticationManager.class.getDeclaredField("jwtParser");
        field.setAccessible(true);
        return (JwtParser) field.get(manager);
    }
}
