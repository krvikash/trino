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

import io.airlift.log.Logger;
import io.starburst.crypto.AesCrypto;
import io.trino.util.Ciphers;

public class GalaxyIdentityCrypto
{
    private static final Logger log = Logger.get(GalaxyIdentityCrypto.class);

    private AesCrypto crypto;

    public GalaxyIdentityCrypto() {}

    public String decryptToUtf8(String input)
    {
        return assureCryptoInitialized().decryptToUtf8(input);
    }

    public String encryptFromUtf8(String input)
    {
        return assureCryptoInitialized().encryptFromUtf8(input);
    }

    private synchronized AesCrypto assureCryptoInitialized()
    {
        if (crypto == null) {
            log.info("About to initialize crypto...");
            crypto = new AesCrypto(Ciphers.createRandomAesEncryptionKey());
            log.info("Finished initializing crypto");
        }
        return crypto;
    }
}
