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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Logger;
import io.airlift.testing.TempFile;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingPortalClient;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.server.starburst.GalaxyCockroachContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.server.starburst.GalaxyImageConstants.STARGATE_DOCKER_REPO;
import static io.trino.server.starburst.GalaxyImageConstants.STARGATE_IMAGE_TAG;
import static io.trino.testing.containers.PemUtils.getCertFile;
import static io.trino.testing.containers.PemUtils.getHostNameFromPem;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofMinutes;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

public class DockerTestingAccountFactory
        implements TestingAccountFactory
{
    private static final Logger log = Logger.get(DockerTestingAccountFactory.class);

    public static final String PORTAL_SERVER_IMAGE = STARGATE_DOCKER_REPO + "portal-server:" + STARGATE_IMAGE_TAG;
    public static final String ACCESS_CONTROL_SERVER_IMAGE = STARGATE_DOCKER_REPO + "access-control-server:" + STARGATE_IMAGE_TAG;
    private static final String API_TESTING_CONFIG_FILE = "/tmp/config.properties";
    private static final String TRINO_TESTING_LAUNCHER_CONFIG_FILE = "/tmp/trino-testing-launcher.properties";
    private static final String TEST_CERT_PORTAL = "TEST_CERT_PORTAL";
    private static final String TEST_CERT_ACCESS_CONTROL = "TEST_CERT_ACCESSCONTROL";
    private static final int PORTAL_PORT = 8888;
    private static final int ACCESS_CONTROL_PORT = 8989;

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final TestingPortalClient testingPortalClient;
    private final String accessControlServerHostName;
    private final int accessControlServerMappedPort;

    public DockerTestingAccountFactory(GalaxyCockroachContainer cockroach)
    {
        this(cockroach, Optional.empty());
    }

    public DockerTestingAccountFactory(GalaxyCockroachContainer cockroach, Optional<Integer> metastorePort)
    {
        try {
            // The pem is always available on the local file system
            File portalPemFile = getCertFile("portal");

            log.info("Running Trino Testing Portal Server");
            GenericContainer<?> portalServer = closer.register(new GenericContainer<>(PORTAL_SERVER_IMAGE));
            portalServer.setNetwork(cockroach.getNetwork());
            portalServer.addEnv(TEST_CERT_PORTAL, Files.readString(portalPemFile.toPath(), UTF_8));
            portalServer.addEnv("AWS_REGION", "us-east-1");
            portalServer.addExposedPort(PORTAL_PORT);

            portalServer.setCommand(
                    "/opt/app/bin/launcher",
                    "run",
                    "-Dhttp-server.https.port=" + PORTAL_PORT,
                    "-Dmcp.base-domain=mcp.gate0.net", // TODO Remove this line once portal-server image includes https://github.com/starburstdata/stargate/pull/28857
                    "-Dmetastore-port=" + metastorePort.orElse(443),
                    "--config",
                    API_TESTING_CONFIG_FILE,
                    "--launcher-config",
                    TRINO_TESTING_LAUNCHER_CONFIG_FILE);

            // Create and write API_TESTING_CONFIG_FILE
            writePropertiesFile(portalServer, API_TESTING_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("db.url", "jdbc:postgresql://cockroach:26257/postgres")
                    .put("db.user", cockroach.getUsername())
                    .put("db.password", cockroach.getPassword())
                    .put("featureflag.cache-expiration-time", "PT0S")
                    .buildOrThrow());

            // Create and write the TRINO_TESTING_LAUNCHER_CONFIG_FILE
            writePropertiesFile(portalServer, TRINO_TESTING_LAUNCHER_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("main-class", "io.starburst.stargate.portal.server.trinotest.TestingTrinoPortalServerMain")
                    .put("process-name", "portal-server")
                    .buildOrThrow());

            portalServer.waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(ofMinutes(4)));
            portalServer.start();

            String portalHostName = getHostNameFromPem(portalPemFile);
            URI portalServerUri = URI.create(format("https://%s:%s", portalHostName, portalServer.getMappedPort(PORTAL_PORT)));

            testingPortalClient = new TestingPortalClient(portalServerUri, closer.register(new JettyHttpClient()));

            // The pem is always available on the local file system
            File accessControlPemFile = getCertFile("accesscontrol");

            log.info("Running Trino Testing Access Control Server");
            GenericContainer<?> accessControlServer = closer.register(new GenericContainer<>(ACCESS_CONTROL_SERVER_IMAGE));
            accessControlServer.setNetwork(cockroach.getNetwork());
            accessControlServer.addEnv(TEST_CERT_ACCESS_CONTROL, Files.readString(accessControlPemFile.toPath(), UTF_8));
            accessControlServer.addExposedPort(ACCESS_CONTROL_PORT);

            accessControlServer.setCommand(
                    "/opt/app/bin/launcher",
                    "run",
                    "-Dhttp-server.https.port=" + ACCESS_CONTROL_PORT,
                    "--config",
                    API_TESTING_CONFIG_FILE,
                    "--launcher-config",
                    TRINO_TESTING_LAUNCHER_CONFIG_FILE);

            // Create and write API_TESTING_CONFIG_FILE
            writePropertiesFile(accessControlServer, API_TESTING_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("db.url", "jdbc:postgresql://cockroach:26257/postgres")
                    .put("db.user", cockroach.getUsername())
                    .put("db.password", cockroach.getPassword())
                    .put("featureflag.cache-expiration-time", "PT0S")
                    .buildOrThrow());

            // Create and write the TRINO_TESTING_LAUNCHER_CONFIG_FILE
            writePropertiesFile(accessControlServer, TRINO_TESTING_LAUNCHER_CONFIG_FILE, ImmutableMap.<String, String>builder()
                    .put("main-class", "io.starburst.stargate.accesscontrol.trinotest.TestingTrinoAccessControlServerMain")
                    .put("process-name", "access-control-server")
                    .buildOrThrow());

            accessControlServer.waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(ofMinutes(4)));
            accessControlServer.start();

            accessControlServerHostName = getHostNameFromPem(accessControlPemFile);
            accessControlServerMappedPort = accessControlServer.getMappedPort(ACCESS_CONTROL_PORT);
        }
        catch (Throwable t) {
            closeAllSuppress(t, closer);
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    @Override
    public TestingAccountClient createAccountClient()
    {
        return testingPortalClient.createAccount();
    }

    @Override
    public URI getAccessControlBaseUri(String accountName)
    {
        return URI.create(format("https://%s.%s:%s", accountName, accessControlServerHostName, accessControlServerMappedPort));
    }

    @Override
    public TrinoSecurityApi getTrinoSecurityApi(String accountName)
    {
        URI accessControlServerUri = getAccessControlBaseUri(accountName);
        return new HttpTrinoSecurityClient(accessControlServerUri, accessControlServerUri, closer.register(new JettyHttpClient()));
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    private void writePropertiesFile(GenericContainer<?> portalServer, String containerPath, Map<String, String> properties)
            throws IOException
    {
        TempFile tempFile = closer.register(new TempFile());
        Files.write(
                tempFile.path(),
                properties.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(toImmutableList()),
                UTF_8);
        portalServer.withFileSystemBind(tempFile.path().toAbsolutePath().toString(), containerPath, BindMode.READ_ONLY);
    }
}
