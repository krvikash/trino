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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.passthrough.TokenPassThroughConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class OAuth2PassThroughSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(OAuth2SecurityConfig.class);
        configBinder(binder).bindConfig(OAuth2PassthroughConfig.class);
        if (buildConfigObject(OAuth2PassthroughConfig.class).isOauth2TokenExchangeEnabled()) {
            binder.bind(SecurityProperties.class).to(OAuth2PassThroughWithTokenExchangeSecurityProperties.class);
        }
        else {
            binder.bind(SecurityProperties.class).to(OAuth2PassThroughSecurityProperties.class);
        }
        configBinder(binder).bindConfig(TokenPassThroughConfig.class, "iceberg.rest-catalog");
    }
}
