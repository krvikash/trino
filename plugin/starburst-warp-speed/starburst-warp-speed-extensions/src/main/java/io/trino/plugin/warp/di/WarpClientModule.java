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
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.trino.plugin.warp.execution.ForWarp;
import io.trino.plugin.warp.execution.WarpClient;
import io.trino.server.security.SecurityConfig;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class WarpClientModule
        extends AbstractConfigurationAwareModule
{
    public WarpClientModule()
    {
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SslContextFactory.Client.class).toInstance(new SslContextFactory.Client(true));
        configBinder(binder).bindConfig(SecurityConfig.class);
        configBinder(binder).bindConfig(NodeConfig.class);
        binder.bind(NodeInfo.class);
        install(internalHttpClientModule("varada", ForWarp.class)
                .build());

        binder.bind(WarpClient.class).in(Scopes.SINGLETON);
    }
}
