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
package io.trino.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.security.SystemAccessControlOperation.CATALOG_MANAGEMENT;
import static io.trino.security.SystemAccessControlOperation.ENTITY_AUTHORIZATION;
import static io.trino.security.SystemAccessControlOperation.QUERY_MANAGEMENT;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameCatalog;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGovernedAwareSystemAccessControl
{
    private static final String USER_NAME = "user_name";
    private static final QueryId QUERY_ID = new QueryId("query_id");
    private static final String ALPHA = "alpha";
    private static final String BETA = "beta";
    private static final String GAMMA = "gamma";

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, GovernedAwareSystemAccessControl.class);
    }

    @Test
    public void testEveryAccessControlIsConsultedWithoutASelector()
    {
        DenySingleCatalogSystemAccessControl refusesAlpha = new DenySingleCatalogSystemAccessControl(ALPHA);
        DenySingleCatalogSystemAccessControl refusesBeta = new DenySingleCatalogSystemAccessControl(BETA);
        AccessControlManager accessControlManager = createAccessControlManager(new AllSystemAccessControlsSelector());
        accessControlManager.setSystemAccessControls(ImmutableList.of(refusesAlpha, refusesBeta));

        SecurityContext securityContext = context();

        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(ALPHA, "a_schema"), ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(BETA, "b_schema"), ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameCatalog(securityContext, ALPHA, BETA))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(accessControlManager.canExecuteFunction(securityContext, new QualifiedObjectName(ALPHA, "a_schema", "a_function"))).isFalse();

        // Each control hides the catalog it refuses and both are consulted about both, so nothing survives
        assertThat(accessControlManager.filterCatalogs(securityContext, ImmutableSet.of(ALPHA, BETA))).isEmpty();
    }

    @Test
    public void testOnlyTheAccessControlsSelectedForACatalogGovernIt()
    {
        // Each catalog selects the control that refuses one of the others, so the control that
        // refuses it is no longer consulted about it
        DenySingleCatalogSystemAccessControl refusesAlpha = new DenySingleCatalogSystemAccessControl(ALPHA);
        DenySingleCatalogSystemAccessControl refusesBeta = new DenySingleCatalogSystemAccessControl(BETA);
        Map<String, SystemAccessControl> catalogAccessControls = ImmutableMap.of(
                ALPHA, refusesBeta,
                BETA, refusesAlpha,
                GAMMA, refusesBeta);
        AccessControlManager accessControlManager = createAccessControlManager(
                selector(catalogAccessControls, ImmutableMap.of()));
        accessControlManager.setSystemAccessControls(ImmutableList.of(refusesAlpha, refusesBeta));

        SecurityContext securityContext = context();

        accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(ALPHA, "a_schema"), ImmutableMap.of());
        accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(BETA, "b_schema"), ImmutableMap.of());
        accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(GAMMA, "c_schema"), ImmutableMap.of());
        assertThat(refusesBeta.catalogsConsultedAbout()).containsExactly(ALPHA, GAMMA);
        assertThat(refusesAlpha.catalogsConsultedAbout()).containsExactly(BETA);

        assertThat(accessControlManager.canExecuteFunction(securityContext, new QualifiedObjectName(ALPHA, "a_schema", "a_function"))).isTrue();
        assertThat(accessControlManager.canExecuteFunction(securityContext, new QualifiedObjectName(BETA, "b_schema", "b_function"))).isTrue();

        // The target does not exist yet, so only the source catalog can route a rename. Each of
        // these would be refused by the control selected for the target
        accessControlManager.checkCanRenameCatalog(securityContext, ALPHA, BETA);
        accessControlManager.checkCanRenameCatalog(securityContext, BETA, ALPHA);

        // Every catalog is visible, because the control that hides it does not govern it
        assertThat(accessControlManager.filterCatalogs(securityContext, ImmutableSet.of(ALPHA, BETA, GAMMA)))
                .containsExactlyInAnyOrder(ALPHA, BETA, GAMMA);

        // Alpha and gamma select the same control, so they are one batched call rather than one each
        assertThat(refusesBeta.filterCatalogsCalls()).isEqualTo(1);
        assertThat(refusesAlpha.filterCatalogsCalls()).isEqualTo(1);
    }

    @Test
    public void testTheSelectedAccessControlStillDecides()
    {
        DenySingleCatalogSystemAccessControl refusesAlpha = new DenySingleCatalogSystemAccessControl(ALPHA);
        DenySingleCatalogSystemAccessControl refusesBeta = new DenySingleCatalogSystemAccessControl(BETA);
        AccessControlManager accessControlManager = createAccessControlManager(selector(
                ImmutableMap.of(ALPHA, refusesAlpha, BETA, refusesBeta),
                ImmutableMap.of()));
        accessControlManager.setSystemAccessControls(ImmutableList.of(refusesAlpha, refusesBeta));

        SecurityContext securityContext = context();

        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(ALPHA, "a_schema"), ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(BETA, "b_schema"), ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanRenameCatalog(securityContext, ALPHA, BETA))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(accessControlManager.filterCatalogs(securityContext, ImmutableSet.of(ALPHA, BETA))).isEmpty();
    }

    @Test
    public void testOperationsAreRoutedByOperationKind()
    {
        SystemAccessControl allowAll = new AllowAllSystemAccessControl();
        SystemAccessControl denyAll = new DenyAllSystemAccessControl();
        AccessControlManager accessControlManager = createAccessControlManager(selector(
                ImmutableMap.of(ALPHA, allowAll),
                ImmutableMap.of(
                        QUERY_MANAGEMENT, allowAll,
                        CATALOG_MANAGEMENT, allowAll,
                        ENTITY_AUTHORIZATION, denyAll)));
        accessControlManager.setSystemAccessControls(ImmutableList.of(allowAll, denyAll));

        Identity identity = Identity.forUser(USER_NAME).build();
        Identity queryOwner = Identity.forUser("query_owner").build();
        SecurityContext securityContext = context();
        TrinoPrincipal principal = new TrinoPrincipal(USER, USER_NAME);

        // Query management selects the permissive control, so the denying one is not consulted
        accessControlManager.checkCanExecuteQuery(identity, QUERY_ID);
        accessControlManager.checkCanViewQueryOwnedBy(identity, queryOwner);
        accessControlManager.checkCanKillQueryOwnedBy(identity, queryOwner);
        assertThat(accessControlManager.filterQueriesOwnedBy(identity, ImmutableSet.of(queryOwner))).containsExactly(queryOwner);

        // Operations with no selection of their own are left to every configured control
        assertThatThrownBy(() -> accessControlManager.checkCanReadSystemInformation(identity))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControlManager.checkCanSetSystemSessionProperty(identity, QUERY_ID, "property"))
                .isInstanceOf(AccessDeniedException.class);

        // Creating a catalog names one that does not exist yet, so it is routed by the operation,
        // while dropping is routed by the catalog it names
        accessControlManager.checkCanCreateCatalog(securityContext, "new_catalog");
        assertThatThrownBy(() -> accessControlManager.checkCanDropCatalog(securityContext, "new_catalog"))
                .isInstanceOf(AccessDeniedException.class);

        // SCHEMA names a catalog, so the check is routed by that catalog rather than by the operation
        accessControlManager.checkCanSetEntityAuthorization(securityContext, new EntityKindAndName("SCHEMA", ImmutableList.of(ALPHA, "a_schema")), principal);

        // ROLE names no catalog, so the control selected for the operation governs it, and that one denies everything
        assertThatThrownBy(() -> accessControlManager.checkCanSetEntityAuthorization(securityContext, new EntityKindAndName("ROLE", ImmutableList.of("a_role")), principal))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    public void testNothingGovernsAllowsTheCheck()
    {
        // A catalog or operation that nothing governs is left unchecked rather than denied
        DenySingleCatalogSystemAccessControl refusesAlpha = new DenySingleCatalogSystemAccessControl(ALPHA);
        AccessControlManager accessControlManager = createAccessControlManager(governsNothing());
        accessControlManager.setSystemAccessControls(ImmutableList.of(refusesAlpha));

        Identity identity = Identity.forUser(USER_NAME).build();
        Identity queryOwner = Identity.forUser("query_owner").build();
        SecurityContext securityContext = context();
        QualifiedObjectName tableName = new QualifiedObjectName(ALPHA, "a_schema", "a_table");

        accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(ALPHA, "a_schema"), ImmutableMap.of());
        accessControlManager.checkCanKillQueryOwnedBy(identity, queryOwner);
        assertThat(accessControlManager.filterQueriesOwnedBy(identity, ImmutableSet.of(queryOwner))).containsExactly(queryOwner);
        assertThat(accessControlManager.filterCatalogs(securityContext, ImmutableSet.of(ALPHA))).containsExactly(ALPHA);
        assertThat(accessControlManager.getRowFilters(securityContext, tableName)).isEmpty();
        assertThat(accessControlManager.getColumnMasks(securityContext, tableName, ImmutableList.of())).isEmpty();
        assertThat(refusesAlpha.catalogsConsultedAbout()).isEmpty();
    }

    @Test
    public void testNoAccessControlConfiguredAllowsEverything()
    {
        List<AccessControlManager> accessControlManagers = ImmutableList.of(
                createAccessControlManager(new AllSystemAccessControlsSelector()),
                createAccessControlManager(governsNothing()));

        SecurityContext securityContext = context();

        for (AccessControlManager accessControlManager : accessControlManagers) {
            accessControlManager.setSystemAccessControls(ImmutableList.of());

            accessControlManager.checkCanImpersonateUser(Identity.forUser("test").build(), "foo");
            accessControlManager.checkCanCreateSchema(securityContext, new CatalogSchemaName(ALPHA, "a_schema"), ImmutableMap.of());
            assertThat(accessControlManager.filterCatalogs(securityContext, ImmutableSet.of(ALPHA, BETA)))
                    .containsExactlyInAnyOrder(ALPHA, BETA);
        }
    }

    private static SystemAccessControlSelector selector(
            Map<String, SystemAccessControl> catalogAccessControls,
            Map<SystemAccessControlOperation, SystemAccessControl> operationAccessControls)
    {
        return new SystemAccessControlSelector()
        {
            @Override
            public boolean governsCatalog(SystemAccessControl accessControl, String catalogName)
            {
                return isSelected(accessControl, catalogAccessControls.get(catalogName));
            }

            @Override
            public boolean governsOperation(SystemAccessControl accessControl, SystemAccessControlOperation operation)
            {
                return isSelected(accessControl, operationAccessControls.get(operation));
            }
        };
    }

    private static boolean isSelected(SystemAccessControl accessControl, SystemAccessControl selected)
    {
        return selected == null || selected == accessControl;
    }

    private static SystemAccessControlSelector governsNothing()
    {
        return new SystemAccessControlSelector()
        {
            @Override
            public boolean governsCatalog(SystemAccessControl accessControl, String catalogName)
            {
                return false;
            }

            @Override
            public boolean governsOperation(SystemAccessControl accessControl, SystemAccessControlOperation operation)
            {
                return false;
            }
        };
    }

    private static SecurityContext context()
    {
        Identity identity = Identity.forUser(USER_NAME).build();
        return new SecurityContext(TransactionId.create(), identity, QUERY_ID, Instant.now());
    }

    private static AccessControlManager createAccessControlManager(SystemAccessControlSelector accessControlSelector)
    {
        return new AccessControlManager(
                NodeVersion.UNKNOWN,
                createTestTransactionManager(),
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME,
                accessControlSelector);
    }

    private static final class DenySingleCatalogSystemAccessControl
            extends DenyAllSystemAccessControl
    {
        private final String refusedCatalog;
        private final Set<String> catalogsConsultedAbout = new HashSet<>();
        private final AtomicInteger filterCatalogsCalls = new AtomicInteger();

        public DenySingleCatalogSystemAccessControl(String refusedCatalog)
        {
            this.refusedCatalog = requireNonNull(refusedCatalog, "refusedCatalog is null");
        }

        public List<String> catalogsConsultedAbout()
        {
            return ImmutableList.copyOf(catalogsConsultedAbout);
        }

        public int filterCatalogsCalls()
        {
            return filterCatalogsCalls.get();
        }

        @Override
        public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
        {
            catalogsConsultedAbout.add(schema.getCatalogName());
            if (schema.getCatalogName().equals(refusedCatalog)) {
                denyCreateSchema(schema.toString());
            }
        }

        @Override
        public boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
        {
            catalogsConsultedAbout.add(catalogName);
            return !catalogName.equals(refusedCatalog);
        }

        @Override
        public void checkCanRenameCatalog(SystemSecurityContext context, String catalog, String newCatalog)
        {
            if (catalog.equals(refusedCatalog)) {
                denyRenameCatalog(catalog, newCatalog);
            }
        }

        @Override
        public boolean canExecuteFunction(SystemSecurityContext context, CatalogSchemaRoutineName functionName)
        {
            return !functionName.getCatalogName().equals(refusedCatalog);
        }

        @Override
        public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
        {
            filterCatalogsCalls.incrementAndGet();
            return catalogs.stream()
                    .filter(Predicate.not(refusedCatalog::equals))
                    .collect(toImmutableSet());
        }
    }

    private static class DenyAllSystemAccessControl
            implements SystemAccessControl {}
}
