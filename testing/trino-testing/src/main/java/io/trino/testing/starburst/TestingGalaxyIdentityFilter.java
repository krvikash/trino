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
package io.trino.testing.starburst;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.server.security.InternalPrincipal;
import io.trino.spi.security.Identity;
import jakarta.annotation.Priority;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.starburst.security.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static io.trino.server.starburst.security.GalaxyIdentity.createIdentity;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static java.util.Objects.requireNonNull;

/**
 * Galaxy query runners don't use the Galaxy authentication system, so the identity is not set up properly.
 * This filter processes the request after the Trino "insecure" authentication filter, and builds a
 * proper Galaxy identity so the downstream Galaxy services (e.g., authorization) work correctly.
 */
@Priority(Priorities.AUTHENTICATION + 1)
public class TestingGalaxyIdentityFilter
        implements ContainerRequestFilter
{
    private final TestingAccountClient accountClient;

    @Inject
    public TestingGalaxyIdentityFilter(TestingAccountClient accountClient)
    {
        this.accountClient = requireNonNull(accountClient, "accountClient is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        if (request.getSecurityContext().getUserPrincipal() instanceof InternalPrincipal) {
            return;
        }

        Optional<Identity> existingIdentity = authenticatedIdentity(request);
        if (existingIdentity.isEmpty()) {
            return;
        }

        // extra credentials are added to identity inside of query submission, so manually process extra credentials from the headers
        Map<String, String> extraCredentials = Optional.ofNullable(request.getHeaders().get("X-Trino-Extra-Credential")).orElseThrow(() -> new BadRequestException("Request expected to have X-Trino-Extra-Credential header"))
                .stream()
                .map(header -> Splitter.on('=').trimResults().splitToList(header))
                .filter(parts -> parts.size() == 2)
                .collect(toImmutableMap(parts -> parts.get(0), parts -> parts.get(1)));
        request.getHeaders().remove("X-Trino-Extra-Credential");

        DispatchSession dispatchSession = accountClient.createDispatchSession(
                new UserId(requiredExtraCredential(extraCredentials, "userId")),
                new RoleId(requiredExtraCredential(extraCredentials, "roleId")));

        Identity identity = createIdentity(
                existingIdentity.get().getUser(),
                dispatchSession.getAccountId(),
                dispatchSession.getUserId(),
                dispatchSession.getRoleId(),
                Optional.empty(),
                dispatchSession.getAccessToken(),
                PORTAL);

        setAuthenticatedIdentity(request, identity);
    }

    private static String requiredExtraCredential(Map<String, String> extraCredentials, String name)
    {
        String value = extraCredentials.get(name);
        if (value == null) {
            throw new ForbiddenException(Response.status(FORBIDDEN).entity("Extra credential missing: " + name).build());
        }
        return value;
    }
}
