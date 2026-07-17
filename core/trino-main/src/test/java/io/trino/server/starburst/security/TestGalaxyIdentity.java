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

import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.UserId;
import org.junit.jupiter.api.Test;

import static io.trino.server.starburst.security.GalaxyIdentity.createPrincipalString;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxyIdentity
{
    @Test
    public void testSerializationCompatibility()
    {
        // Assert the exact serialization format output as downstream Galaxy components will need to parse it
        assertThat(createPrincipalString(new AccountId("a-12345678"), new UserId("u-1234567890"), new RoleId("r-1234567890")))
                .isEqualTo("galaxy:a-12345678:u-1234567890:r-1234567890");
        assertThat(createPrincipalString(new AccountId("a-22345678"), new UserId("u-2234567890"), new RoleId("r-2234567890")))
                .isEqualTo("galaxy:a-22345678:u-2234567890:r-2234567890");
    }
}
