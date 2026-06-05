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
package io.trino.server.security;

import com.google.inject.Inject;
import io.trino.spi.security.Identity;
import jakarta.annotation.Priority;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;

import java.util.List;

import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static jakarta.ws.rs.Priorities.AUTHENTICATION;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

@Priority(AUTHENTICATION)
public class PortalAuthenticationFilter
        implements ContainerRequestFilter
{
    private final PortalAuthenticator portalAuthenticator;

    @Inject
    public PortalAuthenticationFilter(PortalAuthenticator portalAuthenticator)
    {
        this.portalAuthenticator = requireNonNull(portalAuthenticator, "portalAuthenticator is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        try {
            Identity identity = portalAuthenticator.authenticate(request);
            setAuthenticatedIdentity(request, identity);
        }
        catch (AuthenticationException e) {
            sendWwwAuthenticate(
                    request,
                    requireNonNullElse(e.getMessage(), "Unauthorized"),
                    e.getAuthenticateHeader().map(List::of).orElse(List.of()));
        }
    }
}
