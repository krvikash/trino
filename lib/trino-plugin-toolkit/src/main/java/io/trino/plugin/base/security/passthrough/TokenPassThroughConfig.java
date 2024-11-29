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
package io.trino.plugin.base.security.passthrough;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import java.util.Optional;

public class TokenPassThroughConfig
{
    private Optional<IdPName> idpName = Optional.empty();

    public Optional<IdPName> getIdpName()
    {
        return idpName;
    }

    @Config("idp-name")
    @LegacyConfig("idp")
    @ConfigDescription("Name of the preconfigured identity provider, that should be a source of a token")
    public TokenPassThroughConfig setIdpName(String idp)
    {
        this.idpName = Optional.ofNullable(idp).map(IdPName::of);
        return this;
    }
}
