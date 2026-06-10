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
package io.trino.server.starburst.security;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.util.Optional;

import static io.trino.server.starburst.security.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxyTrinoAuthenticator
        extends TestGalaxyAuthenticator
{
    private static final String DEPLOYMENT_ID = "deploymentId";

    @Test
    public void test()
            throws Exception
    {
        KeyPair keyPair = generateKeyPair();
        GalaxyTrinoAuthenticator authenticator = new GalaxyTrinoAuthenticator(new GalaxyAuthenticatorController(TOKEN_ISSUER, ACCOUNT_ID, DEPLOYMENT_ID, keyPair.getPublic()));
        test(DEPLOYMENT_ID, keyPair, authenticator::authenticate);

        // Test missing identity_type assignment still works (is assigned the portal identity_type)
        assertThat(authenticator.authenticate(generateJwt("username", ACCOUNT_ID, DEPLOYMENT_ID, keyPair.getPrivate(), notExpired(), requestNotExpired(), Optional.of("good"), TOKEN_ISSUER, Optional.empty()), Optional.empty()))
                .satisfies(identity -> assertThat(identity.getUser()).isEqualTo("username"))
                .satisfies(identity -> assertThat(identity.getPrincipal().orElseThrow().toString()).isEqualTo(GALAXY_IDENTITY))
                .satisfies(identity -> assertThat(identity.getExtraCredentials().get("identityType")).isEqualTo(PORTAL.name()));
    }
}
