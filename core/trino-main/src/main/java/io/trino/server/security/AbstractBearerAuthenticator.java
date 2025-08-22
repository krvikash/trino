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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import io.jsonwebtoken.JwtException;
import io.trino.spi.security.Identity;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.server.ServletSecurityUtils.getBaseUri;
import static java.lang.String.format;

public abstract class AbstractBearerAuthenticator
        implements Authenticator
{
    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        Supplier<URI> baseUriSupplier = Suppliers.memoize(() -> request.getUriInfo().getBaseUri());
        Supplier<List<String>> headerSupplier = Suppliers.memoize(() -> request.getHeaders().get(AUTHORIZATION));
        return authenticate(extractToken(headerSupplier, baseUriSupplier), baseUriSupplier);
    }

    @Override
    public Identity authenticate(HttpServletRequest request)
            throws AuthenticationException
    {
        Supplier<URI> baseUriSupplier = Suppliers.memoize(() -> getBaseUri(request));
        Supplier<List<String>> headerSupplier = Suppliers.memoize(() -> ImmutableList.copyOf(request.getHeaders(AUTHORIZATION).asIterator()));
        return authenticate(extractToken(headerSupplier, baseUriSupplier), baseUriSupplier);
    }

    public Identity authenticate(String token, Supplier<URI> baseUriSupplier)
            throws AuthenticationException
    {
        try {
            return createIdentity(token).orElseThrow(() -> needAuthentication(baseUriSupplier, Optional.of(token), "Invalid credentials"));
        }
        catch (JwtException | UserMappingException e) {
            throw needAuthentication(baseUriSupplier, Optional.empty(), e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    public String extractToken(Supplier<List<String>> headerExtractor, Supplier<URI> baseUriSupplier)
            throws AuthenticationException
    {
        List<String> headers = headerExtractor.get();
        if (headers == null || headers.isEmpty()) {
            throw needAuthentication(baseUriSupplier, Optional.empty(), null);
        }
        if (headers.size() > 1) {
            throw new IllegalArgumentException(format("Multiple %s headers detected: %s, where only single %s header is supported", AUTHORIZATION, headers, AUTHORIZATION));
        }

        String header = getOnlyElement(headers);
        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(baseUriSupplier, Optional.empty(), null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(baseUriSupplier, Optional.empty(), null);
        }
        return token;
    }

    protected abstract Optional<Identity> createIdentity(String token)
            throws UserMappingException;

    protected abstract AuthenticationException needAuthentication(Supplier<URI> baseUriSupplier, Optional<String> currentToken, String message);
}
