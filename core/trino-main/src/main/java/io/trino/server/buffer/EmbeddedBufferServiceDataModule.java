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
package io.trino.server.buffer;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClient;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import io.starburst.stargate.buffer.discovery.client.ForBufferDiscoveryClient;
import io.starburst.stargate.buffer.discovery.client.HttpDiscoveryClient;
import io.trino.node.InternalCoordinatorLocator;

import java.net.URI;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.starburst.stargate.buffer.data.server.DataServerApplicationModules.getDataServerApplicationModules;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;
import static io.trino.server.buffer.EmbeddedBufferServiceConfig.EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX;

public class EmbeddedBufferServiceDataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(getDataServerApplicationModules(EMBEDDED_BUFFER_SERVICE_CONFIG_PREFIX, true));
        configBinder(binder).bindConfigDefaults(DataServerConfig.class, config -> config.setTraceResourceReportingEnabled(false));

        // discovery client http config with internal communication
        install(internalHttpClientModule("buffer-discovery.http", ForBufferDiscoveryClient.class).build());
    }

    @Inject
    @Provides
    public DiscoveryApi getDiscoveryApi(@ForBufferDiscoveryClient HttpClient httpClient, InternalCoordinatorLocator locator)
    {
        return new HttpDiscoveryClient(
                () -> {
                    Set<URI> coordinatorUris = locator.getCoordinatorUris();
                    if (coordinatorUris.isEmpty()) {
                        throw new IllegalStateException("No coordinators available");
                    }
                    if (coordinatorUris.size() > 1) {
                        throw new IllegalStateException("Multiple coordinators not supported");
                    }
                    return getOnlyElement(coordinatorUris);
                },
                httpClient);
    }
}
