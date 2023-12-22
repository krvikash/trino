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
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorServices;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.ReadOnlySystemAccessControl;
import io.trino.plugin.base.security.testing.TestingSystemAccessControlContext;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControl;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControllerApi;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControllerSupplier;
import io.trino.server.starburst.accesscontrol.GalaxyFunctionScopeResolver;
import io.trino.server.starburst.accesscontrol.StacCatalogManagementAccessControl;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogGovernance;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.testing.TestingConnectorContext;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.metadata.CatalogStatus.OPERATIONAL;
import static io.trino.spi.catalog.CatalogGovernance.DELEGATED;
import static io.trino.spi.catalog.CatalogGovernance.GOVERNED;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestStacBiacDelegatingSystemAccessControlSelector
{
    private static final String GOVERNED_CATALOG = "governed_catalog";
    private static final String DELEGATED_CATALOG = "delegated_catalog";
    private static final String USER_NAME = "user_name";
    private static final QueryId QUERY_ID = new QueryId("query_id");

    private static final CatalogManager CATALOG_MANAGER = catalogManager(ImmutableMap.of(
            GOVERNED_CATALOG, GOVERNED,
            DELEGATED_CATALOG, DELEGATED));

    private final ConsultationRecordingControllerSupplier galaxyConsultations = new ConsultationRecordingControllerSupplier();
    private final GalaxyAccessControl galaxyAccessControl = new GalaxyAccessControl(
            1,
            1,
            galaxyConsultations,
            new GalaxyFunctionScopeResolver(),
            new StacCatalogManagementAccessControl(galaxyConsultations));
    private final RefusingSystemAccessControl delegatedAccessControl = new RefusingSystemAccessControl(DELEGATED_CATALOG);
    private final StacBiacDelegatingSystemAccessControlSelector selector = new StacBiacDelegatingSystemAccessControlSelector(CATALOG_MANAGER);

    @AfterEach
    void shutDownGalaxyAccessControl()
    {
        galaxyAccessControl.shutdown();
    }

    @Test
    void testGovernedCatalogsAreGovernedByGalaxyAlone()
    {
        assertThat(selector.governsCatalog(galaxyAccessControl, GOVERNED_CATALOG)).isTrue();
        assertThat(selector.governsCatalog(delegatedAccessControl, GOVERNED_CATALOG)).isFalse();
        assertThat(selector.governsCatalog(readOnlyAccessControl(), GOVERNED_CATALOG)).isFalse();
    }

    @Test
    void testDelegatedCatalogsAreGovernedByEveryOtherAccessControl()
    {
        assertThat(selector.governsCatalog(galaxyAccessControl, DELEGATED_CATALOG)).isFalse();
        assertThat(selector.governsCatalog(delegatedAccessControl, DELEGATED_CATALOG)).isTrue();
        assertThat(selector.governsCatalog(readOnlyAccessControl(), DELEGATED_CATALOG)).isTrue();
    }

    @Test
    void testCatalogsTheCatalogManagerDoesNotKnowAreTreatedAsGoverned()
    {
        // A name that names no catalog, one that is not a valid catalog name, and one that is empty
        for (String catalogName : ImmutableList.of("missing_catalog", "NotLowercase", "")) {
            assertThat(selector.governsCatalog(galaxyAccessControl, catalogName)).isTrue();
            assertThat(selector.governsCatalog(delegatedAccessControl, catalogName)).isFalse();
        }
    }

    @Test
    void testAGovernedCatalogAndOperationsAreCheckedByGalaxyAlone()
    {
        for (SystemAccessControlOperation operation : SystemAccessControlOperation.values()) {
            assertThat(selector.governsOperation(galaxyAccessControl, operation)).isTrue();
            assertThat(selector.governsOperation(delegatedAccessControl, operation)).isFalse();
        }

        // The delegated control is configured first, so it would have recorded the catalog before Galaxy ever threw
        AccessControlManager accessControlManager = accessControlManager(delegatedAccessControl, galaxyAccessControl);

        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(context(), new CatalogSchemaName(GOVERNED_CATALOG, "a_schema"), ImmutableMap.of()))
                .isInstanceOf(GalaxyConsultedException.class);
        assertThat(galaxyConsultations.consultations()).isEqualTo(1);
        assertThat(delegatedAccessControl.catalogsConsultedAbout()).isEmpty();
    }

    @Test
    void testADelegatedCatalogIsCheckedByEveryOtherAccessControlAlone()
    {
        AccessControlManager accessControlManager = accessControlManager(galaxyAccessControl, delegatedAccessControl);

        assertThatThrownBy(() -> accessControlManager.checkCanCreateSchema(context(), new CatalogSchemaName(DELEGATED_CATALOG, "b_schema"), ImmutableMap.of()))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(delegatedAccessControl.catalogsConsultedAbout()).containsExactly(DELEGATED_CATALOG);
        assertThat(galaxyConsultations.consultations()).isZero();
    }

    @Test
    void testAnOperationNamingNoCatalogIsCheckedByGalaxyAlone()
    {
        // The delegated control is configured first, so it would have recorded the check before Galaxy ever refused
        AccessControlManager accessControlManager = accessControlManager(delegatedAccessControl, galaxyAccessControl);

        assertThatThrownBy(() -> accessControlManager.checkCanImpersonateUser(Identity.forUser(USER_NAME).build(), "another_user"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Galaxy does not support user impersonation");
        assertThat(delegatedAccessControl.impersonationChecks()).isZero();
    }

    @Test
    void testAnAccessControlCannotHideACatalogItDoesNotGovern()
    {
        // The refusing control governs the delegated catalog only
        AccessControlManager accessControlManager = accessControlManager(delegatedAccessControl);

        assertThat(accessControlManager.filterCatalogs(context(), ImmutableSet.of(GOVERNED_CATALOG, DELEGATED_CATALOG)))
                .containsExactly(GOVERNED_CATALOG);
        assertThat(delegatedAccessControl.catalogsFiltered()).containsExactly(DELEGATED_CATALOG);
    }

    private AccessControlManager accessControlManager(SystemAccessControl... accessControls)
    {
        AccessControlManager accessControlManager = new AccessControlManager(
                NodeVersion.UNKNOWN,
                createTestTransactionManager(),
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
                new SecretsResolver(ImmutableMap.of()),
                DefaultSystemAccessControl.NAME,
                selector);
        accessControlManager.setSystemAccessControls(ImmutableList.copyOf(accessControls));
        return accessControlManager;
    }

    private static SystemAccessControl readOnlyAccessControl()
    {
        return new ReadOnlySystemAccessControl.Factory().create(ImmutableMap.of(), new TestingSystemAccessControlContext());
    }

    private static SecurityContext context()
    {
        return new SecurityContext(TransactionId.create(), Identity.forUser(USER_NAME).build(), QUERY_ID, Instant.now());
    }

    private static CatalogManager catalogManager(Map<String, CatalogGovernance> catalogs)
    {
        Map<CatalogName, Catalog> catalogsByName = catalogs.entrySet().stream()
                .collect(toImmutableMap(entry -> new CatalogName(entry.getKey()), entry -> catalog(entry.getKey(), entry.getValue())));

        return new CatalogManager()
        {
            @Override
            public Set<CatalogName> getCatalogNames()
            {
                return catalogsByName.keySet();
            }

            @Override
            public Optional<Catalog> getCatalog(CatalogName catalogName)
            {
                return Optional.ofNullable(catalogsByName.get(catalogName));
            }

            @Override
            public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
            {
                return Optional.empty();
            }

            @Override
            public Set<CatalogHandle> getReachableDynamicCatalogs()
            {
                return catalogsByName.values().stream()
                        .map(Catalog::getCatalogHandle)
                        .collect(toImmutableSet());
            }

            @Override
            public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void renameCatalog(CatalogName catalogName, CatalogName newCatalogName)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void alterCatalog(CatalogName catalogName, Map<String, Optional<String>> properties)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void dropCatalog(CatalogName catalogName, boolean exists)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static Catalog catalog(String catalogName, CatalogGovernance governance)
    {
        CatalogName name = new CatalogName(catalogName);
        CatalogHandle catalogHandle = createRootCatalogHandle(name, new CatalogVersion("1"));
        Connector connector = MockConnectorFactory.create().create(catalogName, ImmutableMap.of(), new TestingConnectorContext());
        ConnectorServices connectorServices = new ConnectorServices(noopTracer(), catalogHandle, connector);
        return new Catalog(
                name,
                catalogHandle,
                new ConnectorName("mock"),
                connectorServices,
                connectorServices,
                connectorServices,
                OPERATIONAL,
                governance);
    }

    /**
     * Records that Galaxy was asked to decide something, and makes the fact visible to the caller.
     */
    private static final class ConsultationRecordingControllerSupplier
            implements GalaxyAccessControllerSupplier
    {
        private final AtomicInteger consultations = new AtomicInteger();

        @Override
        public GalaxyAccessControllerApi apply(Identity identity)
        {
            consultations.incrementAndGet();
            throw new GalaxyConsultedException();
        }

        public int consultations()
        {
            return consultations.get();
        }
    }

    private static final class GalaxyConsultedException
            extends RuntimeException
    {
        private GalaxyConsultedException()
        {
            super("Galaxy access control was consulted");
        }
    }

    /**
     * Refuses everything that names one catalog, and hides it from anyone filtering catalogs, while
     * recording what it was consulted about.
     */
    private static final class RefusingSystemAccessControl
            extends AllowAllSystemAccessControl
    {
        private final String refusedCatalog;
        private final List<String> catalogsConsultedAbout = new CopyOnWriteArrayList<>();
        private final List<String> catalogsFiltered = new CopyOnWriteArrayList<>();
        private final AtomicInteger impersonationChecks = new AtomicInteger();

        public RefusingSystemAccessControl(String refusedCatalog)
        {
            this.refusedCatalog = requireNonNull(refusedCatalog, "refusedCatalog is null");
        }

        public List<String> catalogsConsultedAbout()
        {
            return ImmutableList.copyOf(catalogsConsultedAbout);
        }

        public List<String> catalogsFiltered()
        {
            return ImmutableList.copyOf(catalogsFiltered);
        }

        public int impersonationChecks()
        {
            return impersonationChecks.get();
        }

        @Override
        public void checkCanImpersonateUser(Identity identity, String userName)
        {
            impersonationChecks.incrementAndGet();
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
        public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
        {
            catalogsFiltered.addAll(catalogs);
            return catalogs.stream()
                    .filter(Predicate.not(refusedCatalog::equals))
                    .collect(toImmutableSet());
        }
    }
}
