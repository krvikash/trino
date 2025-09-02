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
package io.trino.server.testing.ai;

import io.starburst.ai.model.ModelConnectionSpecs;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.trino.server.ai.RemoteModelConnectionSpecsLoader.BASE_PATH;
import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path(BASE_PATH)
@ResourceSecurity(INTERNAL_ONLY)
public class TestingModelConnectionSpecsResource
{
    private final ModelConnectionSpecsLoader modelConnectionSpecsLoader;
    private final Set<Integer> callers = Collections.synchronizedSet(new HashSet<>());

    public TestingModelConnectionSpecsResource(ModelConnectionSpecsLoader modelConnectionSpecsLoader)
    {
        this.modelConnectionSpecsLoader = requireNonNull(modelConnectionSpecsLoader, "modelConnectionSpecsLoader is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public ModelConnectionSpecs listAllModels(@Context HttpServletRequest request)
    {
        try {
            return modelConnectionSpecsLoader.load();
        }
        finally {
            // host is not relevant, since all nodes run on the same host
            callers.add(request.getRemotePort());
        }
    }

    // used by tests to ensure the workers had a chance to load the models
    @GET
    @Path("/caller-count")
    @Produces(APPLICATION_JSON)
    public int getCallerCount()
    {
        return callers.size();
    }
}
