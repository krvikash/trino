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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient.GrantDetails;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.trino.server.starburst.security.GalaxyTestHelper;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.function.BiConsumer;

import static com.google.common.base.Verify.verify;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.stargate.accesscontrol.privilege.GrantKind.ALLOW;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_CATALOG;
import static io.starburst.stargate.accesscontrol.privilege.Privilege.CREATE_SCHEMA;
import static io.trino.server.starburst.security.GalaxyTestHelper.FEARLESS_LEADER;
import static io.trino.server.starburst.security.GalaxyTestHelper.LACKEY_FOLLOWER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestStacCatalogManagementAccessControl
{
    private GalaxyTestHelper helper;
    private StacCatalogManagementAccessControl catalogManagementAccessControl;
    private TestingAccountClient accountClient;
    private HttpClient httpClient;
    private RoleId adminRoleId;
    private RoleId fearlessRoleId;
    private RoleId lackeyRoleId;

    @BeforeAll
    public void initialize()
            throws Exception
    {
        helper = new GalaxyTestHelper();
        helper.initialize();
        catalogManagementAccessControl = new StacCatalogManagementAccessControl(helper.getAccessControllerSupplier());
        accountClient = helper.getAccountClient();
        httpClient = new JettyHttpClient();
        adminRoleId = accountClient.getAdminRoleId();
        Map<RoleName, RoleId> roles = helper.getAccessController(helper.adminContext().getIdentity()).listEnabledRoles(helper.adminContext().getIdentity());
        fearlessRoleId = requireNonNull(roles.get(new RoleName(FEARLESS_LEADER)), "Didn't find fearless_leader");
        // Role LACKEY_FOLLOWER is granted to FEARLESS_LEADER
        lackeyRoleId = requireNonNull(roles.get(new RoleName(LACKEY_FOLLOWER)), "Didn't find lackey_follower");
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (httpClient != null) {
            httpClient.close();
        }
        if (helper != null) {
            helper.close();
        }
        httpClient = null;
        helper = null;
        catalogManagementAccessControl = null;
        accountClient = null;
    }

    @Test
    public void testCheckCanShowCreateCatalog()
    {
        // SHOW CREATE CATALOG is gated on catalog visibility, not ownership: a role that can see the
        // catalog (via a privilege on it) may run it, even without owning the catalog.
        String catalogName = "catalog1";
        CatalogId catalogId = helper.getCatalogId(catalogName);
        String deniedMessage = format("Access Denied: Cannot show create catalog for %s.*", catalogName);

        // Without any grant, the catalog is not visible
        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanShowCreateCatalog(helper.context(lackeyRoleId), catalogName))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching(deniedMessage);

        // A catalog privilege makes the catalog visible without granting ownership
        grantUncontainedEntityPrivilege(new GrantDetails(CREATE_SCHEMA, lackeyRoleId, ALLOW, false, catalogId));
        try {
            // Direct grantee can show create catalog
            catalogManagementAccessControl.checkCanShowCreateCatalog(helper.context(lackeyRoleId), catalogName);
            // Indirect grantee: fearless_leader has lackey_follower's role granted to it
            catalogManagementAccessControl.checkCanShowCreateCatalog(helper.context(fearlessRoleId), catalogName);
            // A role with no visibility is still denied
            assertThatThrownBy(() -> catalogManagementAccessControl.checkCanShowCreateCatalog(helper.publicContext(), catalogName))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching(deniedMessage);
        }
        finally {
            accountClient.revokeUncontainedEntityPrivilege(new GrantDetails(CREATE_SCHEMA, lackeyRoleId, ALLOW, false, catalogId));
        }
    }

    @Test
    public void testCheckCanDropCatalog()
    {
        withCatalogOwnedByLackey(
                "catalog2",
                "Cannot drop catalog %s",
                (context, catalog) -> catalogManagementAccessControl.checkCanDropCatalog(context, catalog));
    }

    @Test
    public void testCheckCanRenameCatalog()
    {
        withCatalogOwnedByLackey(
                "catalog3",
                "Cannot rename catalog from %s to renamed_catalog",
                (context, catalog) -> catalogManagementAccessControl.checkCanRenameCatalog(context, catalog, "renamed_catalog"));
    }

    @Test
    public void testCheckCanSetCatalogProperties()
    {
        withCatalogOwnedByLackey(
                "catalog4",
                "Cannot set catalog properties to %s",
                (context, catalog) -> catalogManagementAccessControl.checkCanSetCatalogProperties(context, catalog, Map.of()));
    }

    @Test
    public void testCheckCanCreateCatalog()
    {
        // CREATE_CATALOG is an account-wide privilege, not tied to a specific catalog's ownership
        String catalog = "brand-new-catalog";
        String message = format("Access Denied: Cannot create catalog %s.*", catalog);

        assertThatThrownBy(() -> catalogManagementAccessControl.checkCanCreateCatalog(helper.context(lackeyRoleId), catalog))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching(message);

        accountClient.grantAccountPrivilege(new GrantDetails(CREATE_CATALOG, lackeyRoleId, ALLOW, false, accountClient.getAccountId()));
        try {
            // Direct grantee
            catalogManagementAccessControl.checkCanCreateCatalog(helper.context(lackeyRoleId), catalog);
            // Indirect grantee: fearless_leader has lackey_follower's role granted to it
            catalogManagementAccessControl.checkCanCreateCatalog(helper.context(fearlessRoleId), catalog);
        }
        finally {
            accountClient.revokeAccountPrivilege(new GrantDetails(CREATE_CATALOG, lackeyRoleId, ALLOW, false, accountClient.getAccountId()));
        }
    }

    private void withCatalogOwnedByLackey(String catalogName, String messageTemplate, BiConsumer<SystemSecurityContext, String> consumer)
    {
        CatalogId catalogId = helper.getCatalogId(catalogName);
        String message = format("Access Denied: %s.*", format(messageTemplate, catalogName));

        accountClient.setEntityOwnership(adminRoleId, lackeyRoleId, catalogId);
        try {
            // Direct owner
            consumer.accept(helper.context(lackeyRoleId), catalogName);
            // Indirect owner: fearless_leader has lackey_follower's role granted to it
            consumer.accept(helper.context(fearlessRoleId), catalogName);
            // Non-owner
            assertThatThrownBy(() -> consumer.accept(helper.publicContext(), catalogName))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching(message);
        }
        finally {
            accountClient.setEntityOwnership(adminRoleId, adminRoleId, catalogId);
        }
    }

    // TODO Temporary workaround: in the current stargate version, TestingAccountClient.grantUncontainedEntityPrivilege
    //  POSTs to "uncontainedEntityPrivilege", but the testing resource registers the grant at "grantUncontainedPrivilege"
    //  (that path only accepts DELETE, hence a 405). Revoke is consistent on both sides, so
    //  accountClient.revokeUncontainedEntityPrivilege still works. Remove this once stargate is updated and pulled in.
    private void grantUncontainedEntityPrivilege(GrantDetails grant)
    {
        Request request = preparePost()
                .setUri(uriBuilderFrom(accountClient.getBaseUri()).appendPath("grantUncontainedPrivilege").build())
                .addHeader(AUTHORIZATION, "X-Trino-Plane-Token " + accountClient.getAdminTrinoAccessToken())
                .addHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(GrantDetails.class), grant))
                .build();
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        verify(response.getStatusCode() == 200 || response.getStatusCode() == 204,
                "Expected 200 OK or 204 NO_CONTENT, but got %s from %s",
                response.getStatusCode(),
                request);
    }
}
