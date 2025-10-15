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
package io.trino.filesystem.s3;

import com.google.common.io.Closer;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.io.IOException;
import java.net.URI;

import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_API_PORT;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestS3FileSystemApiTimeout
{
    private static final int TOXIPROXY_CONTROL_PORT = 8474;
    private static final int MINIO_PROXY_PORT = 1234;
    private static final String BUCKET_NAME = "test-bucket-test-s3-file-system-timeout";

    private static final int LATENCY_MS = 3000;

    private String s3Endpoint;

    @AutoClose
    private final Closer closer = Closer.create();

    @BeforeAll
    final void init()
            throws IOException
    {
        Network network = Network.newNetwork();
        closer.register(network::close);
        Minio minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(BUCKET_NAME);
        closer.register(minio::close);

        ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withExposedPorts(TOXIPROXY_CONTROL_PORT, MINIO_PROXY_PORT)
                .withNetwork(network)
                .withNetworkAliases("minio");
        toxiproxy.start();
        closer.register(toxiproxy::close);

        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        Proxy proxy = toxiproxyClient.createProxy("minio", "0.0.0.0:" + MINIO_PROXY_PORT, "minio:" + MINIO_API_PORT);
        proxy.toxics()
                .latency("delayed connection", ToxicDirection.DOWNSTREAM, LATENCY_MS);
        s3Endpoint = URI.create("http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(MINIO_PROXY_PORT)).toString();
    }

    @Test
    public void testCallAttemptTimeout()
    {
        S3FileSystemConfig config = createFileSystemConfig()
                .setMaxErrorRetries(3)
                .setApiCallAttemptTimeout(Duration.valueOf("2s"));

        S3FileSystemStats stats = new S3FileSystemStats();

        TrinoFileSystem fileSystem = new S3FileSystemFactory(OpenTelemetry.noop(), config, stats).create(ConnectorIdentity.forUser("test").build());

        assertThatThrownBy(() -> fileSystem.listFiles(Location.of("s3://%s/".formatted(BUCKET_NAME)))).cause()
                .hasSuppressedException(SdkClientException.create("Request attempt 1 failure: HTTP request execution did not complete before the specified timeout configuration: 2000 millis"));

        AwsSdkV2ApiCallStats callStats = stats.getListObjectsV2();

        assertThat(callStats.getRetries().getTotalCount()).isEqualTo(2);
        assertThat(callStats.getFailures().getTotalCount()).isEqualTo(1);
    }

    @Test
    public void testCallTimeout()
    {
        S3FileSystemConfig config = createFileSystemConfig()
                .setApiCallAttemptTimeout(Duration.valueOf("2s"))
                .setApiCallTimeout(Duration.valueOf("10s"));

        S3FileSystemStats stats = new S3FileSystemStats();

        TrinoFileSystem fileSystem = new S3FileSystemFactory(OpenTelemetry.noop(), config, stats).create(ConnectorIdentity.forUser("test").build());

        assertThatThrownBy(() -> fileSystem.listFiles(Location.of("s3://%s/".formatted(BUCKET_NAME)))).cause()
                .hasMessage("Client execution did not complete before the specified timeout configuration: 10000 millis");

        AwsSdkV2ApiCallStats callStats = stats.getListObjectsV2();

        assertThat(callStats.getRetries().getTotalCount()).isEqualTo(0);
        assertThat(callStats.getFailures().getTotalCount()).isEqualTo(1);
    }

    private S3FileSystemConfig createFileSystemConfig()
    {
        return new S3FileSystemConfig()
                .setEndpoint(s3Endpoint)
                .setRegion(MINIO_REGION)
                .setPathStyleAccess(true)
                .setAwsAccessKey(MINIO_ACCESS_KEY)
                .setAwsSecretKey(MINIO_SECRET_KEY);
    }
}
