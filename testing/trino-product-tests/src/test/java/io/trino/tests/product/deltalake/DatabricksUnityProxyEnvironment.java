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
package io.trino.tests.product.deltalake;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.MountableFile;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Unity catalog on S3 where Trino reaches the Databricks API through an authenticating mitmproxy.
 */
public class DatabricksUnityProxyEnvironment
        extends DatabricksUnityS3Environment
{
    private static final String MITMPROXY_CONTAINER_NAME = "mitmproxy";
    private static final int MITMPROXY_PORT = 8080;
    private static final String MITMPROXY_USERNAME = "proxy_user";
    private static final String MITMPROXY_PASSWORD = "proxy_passwd";
    private static final String MITMPROXY_RESOURCES = "docker/trino-product-tests/common/mitmproxy/";
    private static final String TRUSTSTORE_PATH = "/etc/trino/cert/mitmproxy.jks";
    private static final String TRUSTSTORE_PASSWORD = "changeit";

    private GenericContainer<?> mitmProxy;

    @Override
    protected void startDependencies(Network network)
    {
        mitmProxy = new GenericContainer<>("mitmproxy/mitmproxy:12.3")
                .withNetwork(network)
                .withNetworkAliases(MITMPROXY_CONTAINER_NAME)
                .withClasspathResourceMapping(MITMPROXY_RESOURCES + "mitmproxy-ca.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-ca.pem", BindMode.READ_ONLY)
                .withClasspathResourceMapping(MITMPROXY_RESOURCES + "mitmproxy-ca-cert.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-ca-cert.pem", BindMode.READ_ONLY)
                .withClasspathResourceMapping(MITMPROXY_RESOURCES + "mitmproxy-dhparam.pem", "/home/mitmproxy/.mitmproxy/mitmproxy-dhparam.pem", BindMode.READ_ONLY)
                .withCommand(
                        "mitmdump",
                        "--set",
                        "proxy_debug=true",
                        "--proxyauth",
                        format("%s:%s", MITMPROXY_USERNAME, MITMPROXY_PASSWORD),
                        "--set",
                        "stream_large_bodies=0");
        mitmProxy.start();
    }

    @Override
    protected Map<String, String> hiveCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>(super.hiveCatalogProperties());
        properties.putAll(proxyProperties());
        return properties;
    }

    @Override
    protected Map<String, String> deltaCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>(super.deltaCatalogProperties());
        properties.putAll(proxyProperties());
        return properties;
    }

    @Override
    protected void configureTrinoContainer(TrinoContainer trino)
    {
        super.configureTrinoContainer(trino);
        trino.withEnv("PROXY_HOST", MITMPROXY_CONTAINER_NAME);
        trino.withEnv("PROXY_PORT", Integer.toString(MITMPROXY_PORT));
        trino.withEnv("PROXY_USERNAME", MITMPROXY_USERNAME);
        trino.withEnv("PROXY_PASSWORD", MITMPROXY_PASSWORD);

        // The JDK disables Basic authentication for HTTPS proxy tunnels by default, which makes the CONNECT to mitmproxy fail with 407.
        // The Trino JVM must also trust the mitmproxy certificate, which re-signs the traffic to Databricks.
        // The truststore must keep the default CAs as well, because Trino talks to S3 directly, not through the proxy.
        trino.withCopyFileToContainer(MountableFile.forHostPath(createTruststore(), 0644), TRUSTSTORE_PATH);
        trino.withEnv("JAVA_TOOL_OPTIONS", String.join(
                " ",
                "-Djdk.http.auth.tunneling.disabledSchemes=",
                "-Djavax.net.ssl.trustStore=" + TRUSTSTORE_PATH,
                "-Djavax.net.ssl.trustStorePassword=" + TRUSTSTORE_PASSWORD));
    }

    @Override
    protected Map<String, String> unityMetastoreProperties()
    {
        Map<String, String> properties = new HashMap<>(super.unityMetastoreProperties());
        // The legacy proxy lane never configured a metastore cache TTL
        properties.remove("hive.metastore-cache-ttl");
        return properties;
    }

    @Override
    protected void doClose()
    {
        super.doClose();
        if (mitmProxy != null) {
            mitmProxy.close();
            mitmProxy = null;
        }
    }

    private static Path createTruststore()
    {
        try {
            KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
            truststore.load(null, null);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init((KeyStore) null);
            int index = 0;
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager x509TrustManager) {
                    for (X509Certificate certificate : x509TrustManager.getAcceptedIssuers()) {
                        truststore.setCertificateEntry("default-" + index++, certificate);
                    }
                }
            }

            try (InputStream certificate = DatabricksUnityProxyEnvironment.class.getClassLoader()
                    .getResourceAsStream(MITMPROXY_RESOURCES + "mitmproxy-ca-cert.pem")) {
                truststore.setCertificateEntry("mitmproxy", CertificateFactory.getInstance("X.509").generateCertificate(requireNonNull(certificate, "mitmproxy certificate not found")));
            }

            Path path = Files.createTempFile("mitmproxy-truststore", ".jks");
            path.toFile().deleteOnExit();
            try (OutputStream output = Files.newOutputStream(path)) {
                truststore.store(output, TRUSTSTORE_PASSWORD.toCharArray());
            }
            return path;
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Failed to create truststore", e);
        }
    }

    private static Map<String, String> proxyProperties()
    {
        return Map.of(
                "hive.metastore.unity.proxy.enabled", "true",
                "hive.metastore.unity.proxy.host", "${ENV:PROXY_HOST}",
                "hive.metastore.unity.proxy.port", "${ENV:PROXY_PORT}",
                "hive.metastore.unity.proxy.username", "${ENV:PROXY_USERNAME}",
                "hive.metastore.unity.proxy.password", "${ENV:PROXY_PASSWORD}");
    }
}
