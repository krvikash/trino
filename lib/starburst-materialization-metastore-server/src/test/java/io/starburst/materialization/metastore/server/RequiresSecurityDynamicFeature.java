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
package io.starburst.materialization.metastore.server;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.core.Response;

import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;

/**
 * Test stand-in for a deployment's security {@code DynamicFeature}. It discovers the class-level
 * {@link RequiresSecurity} annotation on a registered {@link MaterializationMetastoreResource} subclass and
 * installs a bearer-token request filter, proving that the abstract resource design lets a deployment plug in
 * its own authentication.
 */
public class RequiresSecurityDynamicFeature
        implements DynamicFeature
{
    public static final String VALID_TOKEN = "test-secret-token";

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        if (resourceInfo.getResourceClass().isAnnotationPresent(RequiresSecurity.class)) {
            context.register(new BearerTokenAuthenticationFilter());
        }
    }

    private static class BearerTokenAuthenticationFilter
            implements ContainerRequestFilter
    {
        @Override
        public void filter(ContainerRequestContext requestContext)
        {
            String authorization = requestContext.getHeaderString(AUTHORIZATION);
            if (!("Bearer " + VALID_TOKEN).equals(authorization)) {
                requestContext.abortWith(Response.status(UNAUTHORIZED).build());
            }
        }
    }
}
