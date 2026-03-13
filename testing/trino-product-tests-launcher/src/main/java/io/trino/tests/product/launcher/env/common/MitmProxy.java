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
package io.trino.tests.product.launcher.env.common;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import org.testcontainers.containers.BindMode;

import java.nio.file.Path;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MitmProxy
        implements EnvironmentExtender
{
    public static final String MITMPROXY_CONTAINER_NAME = "mitmproxy";
    public static final int MITMPROXY_PORT = 8080;
    public static final String MITMPROXY_USERNAME = "proxy_user";
    public static final String MITMPROXY_PASSWORD = "proxy_passwd";

    private final ResourceProvider configDir;

    @Inject
    public MitmProxy(DockerFiles dockerFiles)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("common/mitmproxy");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer proxy = new DockerContainer("mitmproxy/mitmproxy:12.2", MITMPROXY_CONTAINER_NAME);
        proxy
                .withClasspathResourceMapping("docker/trino-product-tests/common/mitmproxy/mitmproxy-ca.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-ca.pem", BindMode.READ_ONLY)
                .withClasspathResourceMapping("docker/trino-product-tests/common/mitmproxy/mitmproxy-ca-cert.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-ca-cert.pem", BindMode.READ_ONLY)
                .withClasspathResourceMapping("docker/trino-product-tests/common/mitmproxy/mitmproxy-dhparam.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-dhparam.pem", BindMode.READ_ONLY)
                .withCommand(
                        "mitmdump",
                        "--set", "proxy_debug=true",
                        "--proxyauth", format("%s:%s", MITMPROXY_USERNAME, MITMPROXY_PASSWORD),
                        "--set", "stream_large_bodies=0");
        builder.addContainer(proxy);
    }

    public Path getCertificatePath()
    {
        return configDir.getPath("mitmproxy.jks");
    }
}
