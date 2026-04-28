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
package io.trino.plugin.warp.it;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import io.trino.plugin.warp.WarpPlugin;
import io.trino.plugin.warp.di.WarpStubsStorageEngineModule;
import io.trino.plugin.warp.dispatcher.DispatcherConnectorFactory;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import javax.crypto.KDF;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.HKDFParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.spec.AlgorithmParameterSpec;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.config.ProxiedConnectorConfig.ICEBERG_CONNECTOR_NAME;
import static io.trino.plugin.warp.config.ProxiedConnectorConfig.PROXIED_CONNECTOR;
import static io.trino.plugin.warp.extension.config.WarpExtensionConfig.USE_HTTP_SERVER_PORT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots a dispatcher with {@code internal-communication.shared-secret} set at the catalog
 * level, then drives the Trino coordinator's internal-bearer flow end-to-end through Jetty
 * with a hand-crafted JWT. Pins:
 * <ul>
 *   <li>Bootstrap survives when the shared-secret config is set
 *       (Guice graph builds, mirrored {@code InternalAuthenticationManager} filter binds).</li>
 *   <li>An HKDF-SHA256 + HS256 JWT signed with {@link #TEST_SHARED_SECRET} is accepted by
 *       trino-main's {@code InternalAuthenticationManager.parseJwt} when delivered over real
 *       HTTP; a JWT signed with a different key is rejected with 401. (The warp mirror's signer
 *       matching this recipe is pinned separately by
 *       {@code TestInternalAuthenticationManagerWireCompatibilityWithTrinoMain}.)</li>
 * </ul>
 */
public class TestDispatcherInternalBearerAuthIT
        extends DispatcherAbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "warp";

    // Must match the secret hardcoded in io.trino.server.testing.TestingTrinoServer
    // (look for "internal-communication.shared-secret"). The Trino coordinator validates
    // internal bearer tokens with that secret, so WarpClient outbound calls must sign with
    // the same value to round-trip.
    private static final String TEST_SHARED_SECRET = "internal-shared-secret";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DispatcherQueryRunner.createQueryRunner(
                new WarpStubsStorageEngineModule(),
                Optional.empty(),
                2,
                Map.of(),
                Map.of("http-server.log.enabled", "false",
                        USE_HTTP_SERVER_PORT, "false",
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        PROXIED_CONNECTOR, ICEBERG_CONNECTOR_NAME,
                        // configures InternalAuthenticationManager to attach JWT on every WarpClient HTTP call
                        "internal-communication.shared-secret", TEST_SHARED_SECRET),
                hiveDir,
                DispatcherConnectorFactory.DISPATCHER_CONNECTOR_NAME,
                CATALOG_NAME,
                new WarpPlugin(),
                ImmutableMap.of());
    }

    /**
     * Verifies that a JWT signed with the correct HKDF-SHA256-derived key is accepted with HTTP 200
     */
    @Test
    public void testRequestWithCorrectSecretIsAccepted()
            throws IOException, InterruptedException
    {
        URI trinoBase = getQueryRunner().getCoordinator().getBaseUrl();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(trinoBase + "/v1/memory"))
                .header("Content-Type", "application/json")
                .header("X-Trino-Internal-Bearer", buildCorrectJwt())
                .GET()
                .build();

        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode())
                    .as("a JWT signed with the correct HKDF-SHA256 key must be accepted by InternalAuthenticationManager")
                    .isEqualTo(HttpURLConnection.HTTP_OK);
        }
    }

    /**
     * Verifies that a JWT signed with the wrong key is rejected with HTTP 401.
     */
    @Test
    public void testRequestWithWrongSecretIsRejected()
            throws IOException, InterruptedException
    {
        URI trinoBase = getQueryRunner().getCoordinator().getBaseUrl();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(trinoBase + "/v1/memory"))
                .header("Content-Type", "application/json")
                .header("X-Trino-Internal-Bearer", buildWrongJwt())
                .GET()
                .build();

        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode())
                    .as("a JWT signed with the wrong HMAC key must be rejected by InternalAuthenticationManager")
                    .isEqualTo(HttpURLConnection.HTTP_UNAUTHORIZED);
        }
    }

    /**
     * Builds a JWT signed with the HKDF-SHA256-derived key for {@link #TEST_SHARED_SECRET},
     * mirroring the key derivation performed by {@link io.trino.plugin.warp.server.InternalAuthenticationManager}.
     */
    private static String buildCorrectJwt()
    {
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");
            AlgorithmParameterSpec params = HKDFParameterSpec.ofExtract()
                    .addIKM(TEST_SHARED_SECRET.getBytes(StandardCharsets.UTF_8))
                    .thenExpand("internal-communication".getBytes(StandardCharsets.UTF_8), 32);
            SecretKey key = hkdf.deriveKey("HmacSHA256", params);
            return buildJwt(key.getEncoded(), "test-node");
        }
        catch (Exception e) {
            throw new RuntimeException("Could not build correct JWT", e);
        }
    }

    /**
     * Builds a JWT whose HMAC key is derived via a plain Guava {@code Hashing.sha256()} hash
     * (and from a different secret entirely), rather than via HKDF-SHA256 as
     * {@link io.trino.plugin.warp.server.InternalAuthenticationManager} does.
     */
    private static String buildWrongJwt()
    {
        byte[] wrongKey = Hashing.sha256()
                .hashString("wrong-secret", StandardCharsets.UTF_8)
                .asBytes();
        return buildJwt(wrongKey, "wrong-node");
    }

    /**
     * Builds a minimal HS256-signed JWT with the given key bytes and subject, using only
     * standard Java crypto APIs (no jjwt dependency required).
     */
    private static String buildJwt(byte[] keyBytes, String subject)
    {
        try {
            Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
            String header = encoder.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            long exp = Instant.now().plusSeconds(60).getEpochSecond();
            String payload = encoder.encodeToString(
                    ("{\"sub\":\"" + subject + "\",\"exp\":" + exp + "}").getBytes(StandardCharsets.UTF_8));
            String signingInput = header + "." + payload;

            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
            String signature = encoder.encodeToString(mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));

            return signingInput + "." + signature;
        }
        catch (Exception e) {
            throw new RuntimeException("Could not build JWT", e);
        }
    }
}
