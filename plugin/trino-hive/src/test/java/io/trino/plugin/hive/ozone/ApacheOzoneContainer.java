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
package io.trino.plugin.hive.ozone;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Map;

import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ApacheOzoneContainer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(ApacheOzoneContainer.class);

    private static final int OFS_ENDPOINT_PORT = 9862;
    private static final int S3G_ENDPOINT_PORT = 9878;
    public static final String DEFAULT_REGION = "us-east-2";
    public static final String DUMMY_ACCESS_KEY = "dummy-access-key";
    public static final String DUMMY_SECRET_KEY = "dummy-secret-key";

    private final DockerComposeContainer apacheOzone;
    private final Path envFilePath;

    public ApacheOzoneContainer(Network network)
    {
        String apacheOzoneResourceLocation = getPathFromClassPathResource("com/starburstdata/presto/plugin/hive/ozone");
        this.envFilePath = Path.of(apacheOzoneResourceLocation + "/.env");

        createEnvFile(
                envFilePath,
                ImmutableMap.of(
                        "NETWORK_ID", network.getId(),
                        "OZONE_ENVIRONMENT_FILE", apacheOzoneResourceLocation + "/environment-variables.env"));

        this.apacheOzone =
                new DockerComposeContainer<>("ozone-", Paths.get(apacheOzoneResourceLocation, "ozone-docker-compose.yml").toFile())
                        .withExposedService("ozone-manager", OFS_ENDPOINT_PORT, Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                        .withExposedService("s3-gateway", S3G_ENDPOINT_PORT)
                        .withServices("ozone-manager", "storage-container-manager", "s3-gateway", "recon")
                        .withScaledService("datanode", 3)
                        .waitingFor("ozone-manager", Wait.forLogMessage(".*HTTP server of ozoneManager listening at.*", 1).withStartupTimeout(Duration.ofMinutes(10)))
                        .withPull(true);
    }

    private static void createEnvFile(Path envFilePath, Map<String, String> envValues)
    {
        try {
            StringBuilder envBuilder = new StringBuilder();
            envValues.forEach((key, value) -> envBuilder.append(key).append("=").append(value).append("\n"));

            Files.deleteIfExists(envFilePath);
            Files.writeString(envFilePath, envBuilder.toString(), UTF_8);
            Files.setPosixFilePermissions(envFilePath, PosixFilePermissions.fromString("r-xr-xr-x"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start()
    {
        apacheOzone.start();
        log.info("Apache Ozone container started with address for S3 Gateway: %s", getS3EndpointAddress());
        log.info("Apache Ozone container started with address for OFS: %s", getOfsEndpointAddress());
    }

    public String getOfsEndpointAddress()
    {
        return "ofs://" + HostAndPort.fromParts("127.0.0.1", apacheOzone.getServicePort("ozone-manager", OFS_ENDPOINT_PORT));
    }

    public String getS3EndpointAddress()
    {
        return "http://" + HostAndPort.fromParts("127.0.0.1", apacheOzone.getServicePort("s3-gateway", S3G_ENDPOINT_PORT));
    }

    @Override
    public void close()
            throws Exception
    {
        apacheOzone.close();
        Files.deleteIfExists(envFilePath);
    }
}
