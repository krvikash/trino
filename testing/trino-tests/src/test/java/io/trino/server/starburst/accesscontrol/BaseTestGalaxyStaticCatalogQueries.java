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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.ServerConfig;
import io.trino.server.starburst.catalogs.CatalogResolver;
import io.trino.server.starburst.catalogs.StaticCatalogResolver;
import io.trino.server.starburst.security.GalaxyCommonSecurityModule;
import io.trino.server.starburst.security.GalaxySecurityHttpClientModule;
import io.trino.server.starburst.security.TestingAccountFactory;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.starburst.TestingGalaxyIdentityFilter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.server.starburst.security.GalaxyIdentity.GalaxyIdentityType.PORTAL;
import static io.trino.server.starburst.security.GalaxyIdentity.createIdentity;
import static io.trino.server.starburst.security.GalaxyTestHelper.GALAXY_STACK_LOCK;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

@ResourceLock(GALAXY_STACK_LOCK)
public abstract class BaseTestGalaxyStaticCatalogQueries
        extends BaseTestGalaxyQueries
{
    @Override
    protected DistributedQueryRunner createGalaxyQueryRunner(
            TestingAccountFactory testingAccountFactory,
            TestingAccountClient testingAccountClient,
            Map<String, String> coordinatorProperties,
            Map<String, String> extraProperties)
            throws Exception
    {
        String catalogNames = Stream.of("tpch", "memory")
                .map(catalogName -> catalogName + "->" + testingAccountClient.getOrCreateCatalog(catalogName))
                .collect(joining(","));
        Map<String, String> accessControlProperties = ImmutableMap.of(
                "galaxy.account-url", testingAccountClient.getBaseUri().toString(),
                "galaxy.access-control-url", testingAccountFactory.getAccessControlBaseUri(testingAccountClient.getAccountName()).toString(),
                "galaxy.catalog-names", catalogNames,
                "galaxy.read-only-catalogs", "tpch");

        Session session = testSessionBuilder()
                .setIdentity(createIdentity(
                        testingAccountClient.getAdminEmail(),
                        testingAccountClient.getAccountId(),
                        testingAccountClient.getAdminUserId(),
                        testingAccountClient.getAdminRoleId(),
                        Optional.empty(),
                        testingAccountClient.getAdminTrinoAccessToken(),
                        PORTAL))
                .setCatalog("memory")
                .setSchema("tiny")
                .build();

        // Queries go over HTTP, where Trino only uses insecure authentication, so no Galaxy authenticator is configured.
        // TestingGalaxyIdentityFilter turns the insecure identity into a Galaxy identity.
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .putAll(extraProperties)
                        .put("hide-inaccessible-columns", "true")
                        .buildOrThrow())
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .putAll(coordinatorProperties)
                        .putAll(accessControlProperties)
                        .buildOrThrow())
                .setSystemAccessControl(GalaxyTrinoSystemAccessFactory.NAME, accessControlProperties)
                .setAdditionalModuleSupplier(() -> new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        binder.bind(TestingAccountClient.class).toInstance(testingAccountClient);
                        jaxrsBinder(binder).bind(TestingGalaxyIdentityFilter.class);
                        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
                            install(new GalaxyCommonSecurityModule());
                            binder.bind(StaticCatalogResolver.class).in(SINGLETON);
                            binder.bind(CatalogResolver.class).to(StaticCatalogResolver.class).in(SINGLETON);
                            install(new GalaxySecurityHttpClientModule());
                            install(new GalaxySystemAccessControlModule());
                        }
                    }
                })
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    @Disabled("cork doesn't support branch access control yet (ENG-29396)")
    @Override
    public void testBranchPrivilegeSql() {}

    @Test
    @Disabled("cork doesn't have the built-in column mask functions yet (ENG-29397)")
    @Override
    public void testSecurityDefinerViewWithColumnMaskReferencingColumnOrRowFilter() {}

    @Test
    @Disabled("cork doesn't have the built-in column mask functions yet (ENG-29397)")
    @Override
    public void testBuiltInColumnMaskFunctionsAreCastToColumnType() {}
}
