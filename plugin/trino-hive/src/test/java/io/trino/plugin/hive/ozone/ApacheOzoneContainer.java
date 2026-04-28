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
import io.trino.plugin.base.util.AutoCloseableCloser;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.testcontainers.lifecycle.Startables.deepStart;

public class ApacheOzoneContainer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(ApacheOzoneContainer.class);

    private static final String OZONE_IMAGE = "apache/ozone:1.4.0";
    private static final int OFS_ENDPOINT_PORT = 9862;
    private static final int S3G_ENDPOINT_PORT = 9878;
    public static final String DEFAULT_REGION = "us-east-2";
    public static final String DUMMY_ACCESS_KEY = "dummy-access-key";
    public static final String DUMMY_SECRET_KEY = "dummy-secret-key";

    private static final Map<String, String> OZONE_ENV = ImmutableMap.<String, String>builder()
            .put("CORE-SITE.XML_fs.defaultFS", "ofs://ozone-manager")
            .put("CORE-SITE.XML_hadoop.security.authentication", "simple")
            .put("CORE-SITE.XML_hadoop.security.authorization", "false")
            .put("CORE-SITE.XML_hadoop.proxyuser.hive.hosts", "*")
            .put("CORE-SITE.XML_hadoop.proxyuser.hive.groups", "*")
            .put("OZONE-SITE.XML_ozone.om.address", "ozone-manager")
            .put("OZONE-SITE.XML_ozone.om.http-address", "ozone-manager:9874")
            .put("OZONE-SITE.XML_ozone.scm.names", "storage-container-manager")
            .put("OZONE-SITE.XML_ozone.scm.datanode.id.dir", "/data")
            .put("OZONE-SITE.XML_ozone.scm.block.client.address", "storage-container-manager")
            .put("OZONE-SITE.XML_ozone.metadata.dirs", "/data/metadata")
            .put("OZONE-SITE.XML_ozone.scm.client.address", "storage-container-manager")
            .put("OZONE-SITE.XML_hdds.datanode.dir", "/data/hdds")
            .put("OZONE-SITE.XML_ozone.recon.db.dir", "/data/metadata/recon")
            .put("OZONE-SITE.XML_ozone.recon.address", "recon:9891")
            .put("OZONE-SITE.XML_ozone.s3g.list-keys.shallow.enabled", "false")
            .put("OZONE-SITE.XML_ozone.scm.container.size", "512MB")
            .put("OZONE-SITE.XML_hdds.datanode.dir.du.reserved", "128MB")
            .put("OZONE-SITE.XML_hdds.datanode.volume.min.free.space", "256MB")
            .put("no_proxy", "ozone-manager,recon,storage-container-manager,s3-gateway,localhost,127.0.0.1")
            .buildOrThrow();

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private final GenericContainer<?> scm;
    private final GenericContainer<?> ozoneManager;
    private final GenericContainer<?> recon;
    private final GenericContainer<?> s3Gateway;
    private final List<GenericContainer<?>> datanodes;

    public ApacheOzoneContainer(Network network)
    {
        String suffix = randomNameSuffix();

        scm = closer.register(createScm(network, suffix));
        ozoneManager = closer.register(createOzoneManager(network, suffix));
        recon = closer.register(createRecon(network, suffix));
        s3Gateway = closer.register(createS3Gateway(network, suffix));
        datanodes = IntStream.rangeClosed(1, 3)
                .mapToObj(index -> closer.register(createDatanode(network, suffix, index)))
                .collect(toImmutableList());
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createScm(Network network, String suffix)
    {
        return new GenericContainer<>(OZONE_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("storage-container-manager")
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withHostName("storage-container-manager")
                        .withName("ozone-scm-" + suffix))
                .withExposedPorts(9876)
                .withEnv(OZONE_ENV)
                .withEnv("ENSURE_SCM_INITIALIZED", "/data/metadata/scm/current/VERSION")
                .withCommand("ozone", "scm")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createOzoneManager(Network network, String suffix)
    {
        return new GenericContainer<>(OZONE_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("ozone-manager")
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withHostName("ozone-manager")
                        .withName("ozone-manager-" + suffix))
                .withExposedPorts(OFS_ENDPOINT_PORT, 9874)
                .withEnv(OZONE_ENV)
                .withEnv("ENSURE_OM_INITIALIZED", "/data/metadata/om/current/VERSION")
                .withEnv("WAITFOR", "storage-container-manager:9876")
                .withCommand("ozone", "om")
                .waitingFor(Wait.forLogMessage(".*HTTP server of ozoneManager listening at.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(10)));
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createRecon(Network network, String suffix)
    {
        return new GenericContainer<>(OZONE_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("recon")
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withHostName("recon")
                        .withName("ozone-recon-" + suffix))
                .withExposedPorts(9888)
                .withEnv(OZONE_ENV)
                .withCommand("ozone", "recon")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createS3Gateway(Network network, String suffix)
    {
        return new GenericContainer<>(OZONE_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("s3-gateway")
                .withCreateContainerCmdModifier(cmd -> cmd
                        .withHostName("s3-gateway")
                        .withName("ozone-s3g-" + suffix))
                .withExposedPorts(S3G_ENDPOINT_PORT)
                .withEnv(OZONE_ENV)
                .withCommand("ozone", "s3g")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> createDatanode(Network network, String suffix, int index)
    {
        return new GenericContainer<>(OZONE_IMAGE)
                .withNetwork(network)
                .withCreateContainerCmdModifier(cmd -> cmd.withName("ozone-datanode-" + index + "-" + suffix))
                .withEnv(OZONE_ENV)
                .withCommand("ozone", "datanode");
    }

    public void start()
    {
        // SCM must be up first — all other services depend on it
        scm.start();

        // Start OM, recon, s3g, and all datanodes in parallel
        deepStart(Stream.concat(
                Stream.of(ozoneManager, recon, s3Gateway),
                datanodes.stream()))
                .join();

        log.info("Apache Ozone container started with address for OFS: %s", getOfsEndpointAddress());
        log.info("Apache Ozone container started with address for S3 Gateway: %s", getS3EndpointAddress());
    }

    public String getOfsEndpointAddress()
    {
        return "ofs://" + HostAndPort.fromParts("127.0.0.1", ozoneManager.getMappedPort(OFS_ENDPOINT_PORT));
    }

    public String getS3EndpointAddress()
    {
        return "http://" + HostAndPort.fromParts("127.0.0.1", s3Gateway.getMappedPort(S3G_ENDPOINT_PORT));
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
