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
package io.trino.server.starburst.accesscontrol;

import io.trino.spi.security.AccessDeniedException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGalaxyCatalogManagementAccessControl
{
    // GalaxyCatalogManagementAccessControl never reads its SystemSecurityContext argument, so null
    // is passed throughout below; if that ever stops being true, these tests will NPE immediately.
    private final GalaxyCatalogManagementAccessControl catalogManagementAccessControl = new GalaxyCatalogManagementAccessControl();

    @Test
    public void testCheckCanShowCreateCatalog()
    {
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanShowCreateCatalog(null, "catalog1"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot show create catalog for catalog1");
    }

    @Test
    public void testCheckCanCreateCatalog()
    {
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanCreateCatalog(null, "catalog1"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot create catalog catalog1");
    }

    @Test
    public void testCheckCanDropCatalog()
    {
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanDropCatalog(null, "catalog1"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot drop catalog catalog1");
    }

    @Test
    public void testCheckCanRenameCatalog()
    {
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanRenameCatalog(null, "catalog1", "catalog2"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot rename catalog from catalog1 to catalog2");
    }

    @Test
    public void testCheckCanSetCatalogProperties()
    {
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanSetCatalogProperties(null, "catalog1", Map.of("key", Optional.of("value"))))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Cannot set catalog properties to catalog1");
    }
}
