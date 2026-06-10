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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.server.security.PortalAuthenticator;
import io.trino.server.starburst.security.GalaxyAuthenticationHelper.RequestBodyHashing;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.util.Optional;

import static io.trino.server.starburst.security.GalaxyAuthenticationHelper.extractToken;
import static jakarta.ws.rs.HttpMethod.POST;
import static jakarta.ws.rs.HttpMethod.PUT;
import static java.util.Objects.requireNonNull;

public class GalaxyTrinoAuthenticator
        implements Authenticator, PortalAuthenticator
{
    private final GalaxyAuthenticatorController controller;

    @Inject
    public GalaxyTrinoAuthenticator(GalaxyAuthenticatorController controller)
    {
        this.controller = requireNonNull(controller, "controller is null");
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        if (request.getMethod().equals(POST) && request.getUriInfo().getRequestUri().getPath().startsWith("/v1/statement")) {
            throw new AuthenticationException("Deprecated API", "Galaxy");
        }

        Optional<RequestBodyHashing> requestBodyHashing;
        if (request.getMethod().equals(PUT) && request.getUriInfo().getRequestUri().getPath().startsWith("/v1/statement")) {
            requestBodyHashing = Optional.of(new RequestBodyHashing(request, "statement_hash"));
        }
        else {
            requestBodyHashing = Optional.empty();
        }
        return authenticate(extractToken(request).orElseThrow(() -> new AuthenticationException("Galaxy token is required", "Galaxy")), requestBodyHashing);
    }

    @VisibleForTesting
    public Identity authenticate(String token, Optional<RequestBodyHashing> requestBodyHashing)
            throws AuthenticationException
    {
        return controller.authenticate(token, requestBodyHashing);
    }
}
