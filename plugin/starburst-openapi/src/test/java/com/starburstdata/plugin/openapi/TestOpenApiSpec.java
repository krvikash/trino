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
package com.starburstdata.plugin.openapi;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatNoException;

final class TestOpenApiSpec
{
    @Test
    public void testLoadsGithub()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("github.json"));
    }

    @Test
    public void testLoadsGithubPatched()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("github-patched.json"));
    }

    @Test
    public void testLoadsJira()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("jira.json"));
    }

    @Test
    public void testLoadsGalaxy()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("galaxy.json"));
    }

    @Test
    public void testLoadsPetstore()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("petstore.yaml"));
    }

    @Test
    public void testLoadsDatadog()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("datadog.yaml"));
    }

    @Test
    public void testLoadsCloudflare()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("cloudflare.json"));
    }

    @Test
    public void testLoadsOpenMeteo()
    {
        assertThatNoException().isThrownBy(() -> loadSpec("openmeteo.yml"));
    }

    private OpenApiSpec loadSpec(String name)
    {
        OpenApiConfig config = new OpenApiConfig();
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(name));
        config.setSpecLocation(specResource.getFile());
        return new OpenApiSpec(config);
    }
}
