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
package io.starburst.materialization.metastore.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.starburst.materialization.metastore.server.db.MaterializationMetastoreDbModule;
import io.starburst.materialization.metastore.server.db.TestingDbModule;
import io.starburst.stargate.id.AccountId;

import java.net.URI;
import java.util.List;
import java.util.Map;

public class TestingMaterializationMetastoreServer
        implements AutoCloseable
{
    /**
     * Each metastore is owned by an account derived from its id, so tenants in tests do not share an account.
     */
    public static final MaterializationAccountResolver TESTING_ACCOUNT_RESOLVER = metastoreId -> testingAccountId(metastoreId.id());

    /**
     * The account {@link #TESTING_ACCOUNT_RESOLVER} resolves for {@code metastoreId}, for clients that must send it.
     */
    public static AccountId testingAccountId(String metastoreId)
    {
        return new AccountId("a-%08d".formatted(10_000_000 + Math.floorMod(metastoreId.hashCode(), 90_000_000)));
    }

    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;
    private final Injector injector;

    public TestingMaterializationMetastoreServer(String dbUrl, String dbUser, String dbPassword)
    {
        this(dbUrl, dbUser, dbPassword, ImmutableMap.of());
    }

    public TestingMaterializationMetastoreServer(String dbUrl, String dbUser, String dbPassword, Map<String, String> extraProperties)
    {
        this(dbUrl, dbUser, dbPassword, extraProperties, NoAuthMaterializationMetastoreResource.class, ImmutableList.of());
    }

    /**
     * @param materializationMetastoreResourceClass concrete resource subclass declaring the server's authentication posture
     * @param securityModules additional modules that wire the security enforcement (for example a {@code DynamicFeature})
     */
    public TestingMaterializationMetastoreServer(
            String dbUrl,
            String dbUser,
            String dbPassword,
            Map<String, String> extraProperties,
            Class<? extends MaterializationMetastoreResource> materializationMetastoreResourceClass,
            List<Module> securityModules)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("metastore.jdbc.url", dbUrl)
                .put("metastore.jdbc.user", dbUser)
                .put("metastore.jdbc.password", dbPassword)
                .putAll(extraProperties)
                .buildOrThrow();
        List<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule("materialization-metastore"))
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new TestingDbModule())
                .add(new MaterializationMetastoreDbModule())
                .add(new MaterializationMetastoreServerModule(materializationMetastoreResourceClass))
                .add(binder -> binder.bind(MaterializationAccountResolver.class).toInstance(TESTING_ACCOUNT_RESOLVER))
                .addAll(securityModules)
                .build();
        Bootstrap app = new Bootstrap(modules);
        this.injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(properties)
                .initialize();
        this.lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        this.server = injector.getInstance(TestingHttpServer.class);
    }

    public URI baseUri()
    {
        return server.getBaseUrl();
    }

    @Override
    public void close()
    {
        lifeCycleManager.stop();
    }

    public Injector getInjector()
    {
        return injector;
    }
}
