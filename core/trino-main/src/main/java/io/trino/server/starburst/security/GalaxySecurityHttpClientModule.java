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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.airlift.units.Duration;
import io.starburst.stargate.accesscontrol.client.HttpTrinoSecurityClient;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControlUrlsConfig;

import java.net.URI;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class GalaxySecurityHttpClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GalaxyAccessControlUrlsConfig.class);
        bindHttpClient(binder);
    }

    public static void bindHttpClient(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("galaxy-readwrite-access-control", ForGalaxyReadWriteSystemAccessControl.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(20, SECONDS));
                    // Automatic https is disabled because this client is for external
                    // communication back to the Galaxy portal.
                    config.setAutomaticHttpsSharedSecret(null);
                    // Using 3 times the default, as domain is always the same, so all connections hit same server
                    // previously used default (20) was throttling requests, doing 3x20 should serve as new conservative default
                    // this is subject to increase if we see throttling still, or decrease if we overwhelm RBAC
                    config.setMaxConnectionsPerServer(60);
                });

        httpClientBinder(binder).bindHttpClient("galaxy-regional-access-control", ForGalaxyRegionalAccessControl.class)
                .withConfigDefaults(config -> {
                    // Regional access control is geared toward low-latency reads (not writes), so we expect a shorter request with a smaller timeout
                    config.setIdleTimeout(new Duration(15, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setAutomaticHttpsSharedSecret(null);
                    config.setMaxConnectionsPerServer(80);
                });
    }

    @Provides
    @Singleton
    public static TrinoSecurityApi createTrinoSecurityApi(
            @ForGalaxyReadWriteSystemAccessControl HttpClient readWriteHttpClient,
            @ForGalaxyRegionalAccessControl HttpClient regionalHttpClient,
            GalaxyAccessControlUrlsConfig config)
    {
        requireNonNull(config, "config is null");
        URI writeUri = config.getAccessControlOverrideUri();
        URI readUri = config.getRegionalAccessControlUri().orElse(writeUri);
        HttpClient regionalHttpClientToUse = config.getRegionalAccessControlUri().map(_ -> regionalHttpClient).orElse(readWriteHttpClient);
        return new HttpTrinoSecurityClient(writeUri, readUri, readWriteHttpClient, regionalHttpClientToUse);
    }
}
