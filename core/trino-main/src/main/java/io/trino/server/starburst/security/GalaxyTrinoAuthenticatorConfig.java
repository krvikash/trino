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

import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotEmpty;

import java.io.File;

public class GalaxyTrinoAuthenticatorConfig
{
    private String accountId;
    private String deploymentId;
    private String tokenIssuer;
    private String publicKey;
    private File publicKeyFile;

    @NotEmpty
    public String getAccountId()
    {
        return accountId;
    }

    @Config("galaxy.account-id")
    public GalaxyTrinoAuthenticatorConfig setAccountId(String accountId)
    {
        this.accountId = accountId;
        return this;
    }

    @NotEmpty
    public String getDeploymentId()
    {
        return deploymentId;
    }

    @Config("galaxy.deployment-id")
    public GalaxyTrinoAuthenticatorConfig setDeploymentId(String deploymentId)
    {
        this.deploymentId = deploymentId;
        return this;
    }

    @NotEmpty
    public String getTokenIssuer()
    {
        return tokenIssuer;
    }

    @Config("galaxy.authentication.token-issuer")
    public GalaxyTrinoAuthenticatorConfig setTokenIssuer(String tokenIssuer)
    {
        this.tokenIssuer = tokenIssuer;
        return this;
    }

    public String getPublicKey()
    {
        return publicKey;
    }

    @Config("galaxy.authentication.public-key")
    public GalaxyTrinoAuthenticatorConfig setPublicKey(String publicKey)
    {
        this.publicKey = publicKey;
        return this;
    }

    @FileExists
    public File getPublicKeyFile()
    {
        return publicKeyFile;
    }

    @Config("galaxy.authentication.public-key-file")
    public GalaxyTrinoAuthenticatorConfig setPublicKeyFile(File publicKeyFile)
    {
        this.publicKeyFile = publicKeyFile;
        return this;
    }
}
