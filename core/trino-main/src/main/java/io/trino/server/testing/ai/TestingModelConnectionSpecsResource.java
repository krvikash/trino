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
import io.starburst.ai.model.ModelConnectionSpecsLoader;
import io.trino.server.security.ResourceSecurity;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import static io.trino.server.security.ResourceSecurity.AccessType.INTERNAL_ONLY;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/api/v1/ai/internal/model-connection-specs")
@ResourceSecurity(INTERNAL_ONLY)
public class TestingModelConnectionSpecsResource
{
    private final ModelConnectionSpecsLoader modelConnectionSpecsLoader;

    public TestingModelConnectionSpecsResource(ModelConnectionSpecsLoader modelConnectionSpecsLoader)
    {
        this.modelConnectionSpecsLoader = requireNonNull(modelConnectionSpecsLoader, "modelConnectionSpecsLoader is null");
    }

    @GET
    @Produces(APPLICATION_JSON)
    public ModelConnectionSpecs listAllModels()
    {
        return modelConnectionSpecsLoader.load();
    }
}
