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
package io.trino.plugin.elasticsearch;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.elasticsearch.client.AwsSecurityRestClientConfigurator;
import io.trino.plugin.elasticsearch.client.BackpressureRestClient;
import io.trino.plugin.elasticsearch.client.BasicSecurityRestClientConfigurator;
import io.trino.plugin.elasticsearch.client.ElasticRestClientConfigurator;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.ElasticsearchClientStats;
import io.trino.plugin.elasticsearch.ptf.RawQuery;
import io.trino.plugin.elasticsearch.substitution.ElasticsearchSubstitutionMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchConfig.SecurityOptions.AWS;
import static io.trino.plugin.elasticsearch.ElasticsearchConfig.SecurityOptions.PASSWORD;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_SSL_INITIALIZATION_FAILURE;
import static java.lang.StrictMath.toIntExact;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ElasticsearchConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSubstitutionMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(NodesSystemTable.class).in(Scopes.SINGLETON);

        binder.bind(ElasticsearchClientStats.class).in(Scopes.SINGLETON);

        // To be backward compatible.
        newExporter(binder).export(ElasticsearchClientStats.class).as(objectNameGenerator -> objectNameGenerator.generatedNameOf(ElasticsearchClient.class));

        binder.bind(BackpressureRestClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ElasticsearchConfig.class);

        newOptionalBinder(binder, AwsSecurityConfig.class);
        newOptionalBinder(binder, PasswordConfig.class);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(RawQuery.class).in(Scopes.SINGLETON);

        Multibinder<ElasticRestClientConfigurator> configurators = newSetBinder(binder, ElasticRestClientConfigurator.class);

        ElasticsearchConfig config = buildConfigObject(ElasticsearchConfig.class);

        if (config.getSecurity().isPresent()) {
            switch (config.getSecurity().orElseThrow()) {
                case AWS -> {
                    configBinder(binder).bindConfig(AwsSecurityConfig.class);
                    configurators.addBinding().to(AwsSecurityRestClientConfigurator.class).in(Scopes.SINGLETON);
                }
                case PASSWORD -> {
                    configBinder(binder).bindConfig(PasswordConfig.class);
                    configurators.addBinding().to(BasicSecurityRestClientConfigurator.class).in(Scopes.SINGLETON);
                }
            }
        }
    }

    @Provides
    @Singleton
    public RestClient createRestClientBuilder(
            ElasticsearchConfig config,
            Set<ElasticRestClientConfigurator> clientConfigurators)
    {
        RestClientBuilder builder = RestClient.builder(
                config.getHosts().stream()
                        .map(httpHost -> new HttpHost(httpHost, config.getPort(), config.isTlsEnabled() ? "https" : "http"))
                        .toArray(HttpHost[]::new));

        builder.setHttpClientConfigCallback(_ -> {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(toIntExact(config.getConnectTimeout().toMillis()))
                    .setSocketTimeout(toIntExact(config.getRequestTimeout().toMillis()))
                    .build();

            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            // the client builder passed to the call-back is configured to use system properties, which makes it
            // impossible to configure concurrency settings, so we need to build a new one from scratch
            HttpAsyncClientBuilder clientBuilder = HttpAsyncClientBuilder.create()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultIOReactorConfig(reactorConfig)
                    .setMaxConnPerRoute(config.getMaxHttpConnections())
                    .setMaxConnTotal(config.getMaxHttpConnections());
            if (config.isTlsEnabled()) {
                buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword())
                        .ifPresent(clientBuilder::setSSLContext);

                if (!config.isVerifyHostnames()) {
                    clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
            }

            clientConfigurators.forEach(configurator -> configurator.configure(clientBuilder));

            return clientBuilder;
        });

        return builder.build();
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(ELASTICSEARCH_SSL_INITIALIZATION_FAILURE, e);
        }
    }
}
