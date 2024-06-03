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
package io.trino.plugin.warp.di;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.units.Duration;
import io.trino.plugin.warp.execution.ForWarp;
import io.trino.plugin.warp.execution.WarpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.Map;

import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.concurrent.TimeUnit.MINUTES;

public class WarpClientModule
        implements WarpBaseModule
{
    private static final String HTTP_CLIENT_IDLE_TIMEOUT = "http.idle-timeout";
    private static final String HTTP_CLIENT_REQUEST_TIMEOUT = "http.request-timeout";
    private final Map<String, String> config;

    public WarpClientModule(Map<String, String> config)
    {
        this.config = config;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(SslContextFactory.Client.class).toInstance(new SslContextFactory.Client(true));
        int idleTimeout = Integer.parseInt(config.getOrDefault(HTTP_CLIENT_IDLE_TIMEOUT, "240"));
        int requestTimeout = Integer.parseInt(config.getOrDefault(HTTP_CLIENT_REQUEST_TIMEOUT, "240"));
        httpClientBinder(binder).bindHttpClient("varada", ForWarp.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(idleTimeout, MINUTES));
                    config.setRequestTimeout(new Duration(requestTimeout, MINUTES));
                    config.setMaxConnectionsPerServer(250);
                });
        binder.bind(WarpClient.class).in(Scopes.SINGLETON);
    }
}
