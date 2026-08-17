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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import io.starburst.stargate.accesscontrol.client.CatalogSchemaName;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.client.testing.TestUser;
import io.starburst.stargate.accesscontrol.client.testing.TestingAccountClient;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.server.starburst.GalaxyCockroachContainer;
import io.trino.server.starburst.GalaxyEnabledConfig;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControl;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControllerApi;
import io.trino.server.starburst.accesscontrol.GalaxyAccessControllerSupplier;
import io.trino.server.starburst.accesscontrol.GalaxyAccountPermissionsCache;
import io.trino.server.starburst.accesscontrol.GalaxyCatalogDdlObserver;
import io.trino.server.starburst.accesscontrol.GalaxyFunctionScopeResolver;
import io.trino.server.starburst.accesscontrol.GalaxyPermissionsCache;
import io.trino.server.starburst.accesscontrol.GalaxySecurityMetadata;
import io.trino.server.starburst.accesscontrol.GalaxySystemAccessControlConfig;
import io.trino.server.starburst.accesscontrol.LazyGalaxyAccessControllerSupplier;
import io.trino.server.starburst.accesscontrol.StacCatalogManagementAccessControl;
import io.trino.server.starburst.catalogs.StaticCatalogResolver;
import io.trino.server.starburst.security.GalaxyIdentity.GalaxyIdentityType;
import io.trino.spi.QueryId;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingSession;
import io.trino.transaction.TransactionId;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.server.starburst.accesscontrol.MetadataAccessControllerSupplier.TRANSACTION_ID_KEY;
import static io.trino.server.starburst.security.GalaxyIdentity.createIdentity;
import static io.trino.server.starburst.security.GalaxyIdentity.createPrincipalString;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.server.starburst.security.TestingAccountFactory.createTestingAccountFactory;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GalaxyTestHelper
        implements AutoCloseable
{
    public static final String ACCOUNT_ADMIN = "accountadmin";
    public static final String PUBLIC = "public";
    public static final String FEARLESS_LEADER = "fearless_leader";
    public static final String LACKEY_FOLLOWER = "lackey_follower";

    /*
     * TODO:
     *  This is the name of the Galaxy enum defined at io.starburst.stargate.model.CatalogKind.GALAXY_CATALOG
     *  Added here to remove dependency on the Galaxy stargate-common module
     *  Re-import the Galaxy enum once it has been stripped out of stargate-common module
     */
    private static final String GALAXY_CATALOG_ENUM_NAME = "GALAXY_CATALOG";

    private GalaxyCockroachContainer cockroach;
    private TestingAccountFactory accountFactory;
    private GalaxySystemAccessControlConfig systemAccessControlConfig;
    private GalaxyAccessControllerSupplier accessControllerSupplier;
    private TestingAccountClient accountClient;
    private GalaxyAccessControl accessControl;
    private GalaxySecurityMetadata metadataApi;
    private TrinoSecurityApi client;
    private StaticCatalogResolver catalogResolver;
    private GalaxyEnabledConfig galaxyEnabledConfig;

    private final AtomicInteger queryIds = new AtomicInteger();

    private LoadingCache<UserIdAndRoleId, DispatchSession> dispatchSessionCache;

    public void initialize()
            throws Exception
    {
        initialize(false);
    }

    public void initialize(boolean enableSharedCache)
    {
        initialize(enableSharedCache, ImmutableSet.of());
    }

    public void initialize(boolean enableSharedCache, Set<String> accountFeatureFlags)
    {
        cockroach = new GalaxyCockroachContainer();
        accountFactory = createTestingAccountFactory(() -> cockroach);
        accountClient = accountFactory.createAccountClient();

        // Enable account feature flags before issuing any access-control-server request below (createRole, etc.).
        // The portal-server and access-control-server run as Docker containers with independent feature-flag caches over
        // the shared database. The testing config sets featureflag.cache-expiration-time=PT0S, but Guava's
        // expireAfterWrite treats a zero duration as "never expire" rather than "no caching", so the access-control-server
        // permanently caches an account's flags on first read. Upserting here goes through the portal-server to the shared
        // database before the access-control-server has read (and cached) the flags, so its first load picks them up.
        accountFeatureFlags.forEach(flag -> accountClient.upsertAccountFeatureFlag(flag, true));

        // creating auth keys is very slow so cache them
        // todo figure out why this is slow
        dispatchSessionCache = buildNonEvictableCache(
                CacheBuilder.newBuilder(),
                CacheLoader.from(userIdAndRoleId -> accountClient.createDispatchSession(userIdAndRoleId.userId(), userIdAndRoleId.roleId())));

        Set<String> catalogNames = ImmutableSet.of("catalog1", "catalog2", "catalog3", "catalog4", "catalog5", "tpch");
        Map<String, CatalogId> catalogs = catalogNames.stream()
                .collect(Collectors.toMap(Function.identity(), accountClient::getOrCreateCatalog));

        // Create sample function to create galaxy catalog
        accountClient.createFunction(
                "functions",
                "sample",
                "token_sample",
                "sample() RETURNS int RETURN 1",
                List.of(new CatalogSchemaName("galaxy", "functions")),
                Optional.empty(),
                false);

        CatalogId galaxyCatalogId = accountClient.getGalaxyCatalog(GALAXY_CATALOG_ENUM_NAME);
        catalogs.put("galaxy", galaxyCatalogId);

        catalogResolver = new StaticCatalogResolver(ImmutableBiMap.copyOf(catalogs), ImmutableSet.of("tpch"), ImmutableMap.of(), ImmutableSetMultimap.of());
        client = accountFactory.getTrinoSecurityApi(accountClient.getAccountName());
        systemAccessControlConfig = new GalaxySystemAccessControlConfig()
                .setBackgroundProcessingThreads(8)
                .setVisibilityBatchSize(1_000);
        GalaxyPermissionsCache permissionsCache = new GalaxyPermissionsCache(systemAccessControlConfig);
        GalaxyAccountPermissionsCache accountPermissionsCache = new GalaxyAccountPermissionsCache(catalogResolver, systemAccessControlConfig, client);
        galaxyEnabledConfig = new GalaxyEnabledConfig().setGalaxyEnabled(enableSharedCache);
        accessControllerSupplier = new LazyGalaxyAccessControllerSupplier(client, catalogResolver, permissionsCache, accountPermissionsCache, systemAccessControlConfig, galaxyEnabledConfig);
        accessControl = new GalaxyAccessControl(
                systemAccessControlConfig.getBackgroundProcessingThreads(),
                systemAccessControlConfig.getVisibilityBatchSize(),
                accessControllerSupplier,
                new GalaxyFunctionScopeResolver(),
                new StacCatalogManagementAccessControl(accessControllerSupplier));
        metadataApi = new GalaxySecurityMetadata(client, catalogResolver, accessControllerSupplier, new GalaxyFunctionScopeResolver(), new GalaxyCatalogDdlObserver());

        // Make the roles
        metadataApi.createRole(adminSession(), FEARLESS_LEADER, Optional.empty());
        metadataApi.createRole(adminSession(), LACKEY_FOLLOWER, Optional.empty());
        metadataApi.grantRoles(adminSession(), ImmutableSet.of(LACKEY_FOLLOWER), ImmutableSet.of(new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER)), false, Optional.empty());
    }

    @Override
    public void close()
            throws Exception
    {
        if (cockroach != null) {
            cockroach.close();
            cockroach = null;
        }
        if (accountFactory != null) {
            accountFactory.close();
        }
        if (accessControl != null) {
            accessControl.shutdown();
        }
    }

    public GalaxySystemAccessControlConfig getSystemAccessControlConfig()
    {
        return systemAccessControlConfig;
    }

    public GalaxyCockroachContainer getCockroach()
    {
        return cockroach;
    }

    public TestingAccountClient getAccountClient()
    {
        return accountClient;
    }

    public URI getAccessControlBaseUri()
    {
        return accountFactory.getAccessControlBaseUri(accountClient.getAccountName());
    }

    public GalaxyAccessControllerApi getAccessController(Identity identity)
    {
        return accessControllerSupplier.apply(identity);
    }

    public GalaxyAccessControllerSupplier getAccessControllerSupplier()
    {
        return accessControllerSupplier;
    }

    public GalaxyAccessControl getAccessControl()
    {
        return accessControl;
    }

    public TrinoSecurityApi getClient()
    {
        return client;
    }

    public GalaxySecurityMetadata getMetadataApi()
    {
        return metadataApi;
    }

    public StaticCatalogResolver getCatalogResolver()
    {
        return catalogResolver;
    }

    public void setUseSharedPermissionsCache(boolean useSharedPermissionsCache)
    {
        galaxyEnabledConfig.setUseSharedPermissionsCache(useSharedPermissionsCache);
    }

    public CatalogId getCatalogId(String catalogName)
    {
        return catalogResolver.getCatalogId(Optional.empty(), catalogName).orElseThrow(() -> new IllegalArgumentException("Unknown catalog " + catalogName));
    }

    public SystemSecurityContext context(RoleId roleId)
    {
        return context(session(roleId));
    }

    public Map<RoleName, RoleId> getActiveRoles(SystemSecurityContext context)
    {
        return accessControllerSupplier.apply(context.getIdentity()).listEnabledRoles(context.getIdentity());
    }

    public void checkAccess(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        successfulContexts.forEach(context -> {
            try {
                consumer.accept(context);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Failed unexpectedly", message, context, e);
            }
        });

        failingContexts.forEach(context -> {
            try {
                assertThatThrownBy(() -> consumer.accept(context))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessage(message);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Succeeded unexpectedly", message, context, e);
            }
        });
    }

    public void checkAccessMatching(String message, List<SystemSecurityContext> successfulContexts, List<SystemSecurityContext> failingContexts, Consumer<SystemSecurityContext> consumer)
    {
        successfulContexts.forEach(context -> {
            try {
                consumer.accept(context);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Failed unexpectedly", message, context, e);
            }
        });

        failingContexts.forEach(context -> {
            try {
                assertThatThrownBy(() -> consumer.accept(context))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessageMatching(message);
            }
            catch (Throwable e) {
                handleUnexpectedResult("Succeeded unexpectedly", message, context, e);
            }
        });
    }

    private void handleUnexpectedResult(String description, String message, SystemSecurityContext context, Throwable e)
    {
        Map<String, String> credentials = context.getIdentity().getExtraCredentials();
        RoleId roleId = new RoleId(credentials.get("roleId"));
        BiMap<RoleName, RoleId> roles = HashBiMap.create(client.listRoles(toDispatchSession(adminSession())));
        RoleName roleName = roles.inverse().get(roleId);
        throw new AssertionError(format("%s, expected message %s, roleName %s, roleId %s", description, message, roleName, roleId), e);
    }

    public Identity roleNameToIdentity(String roleName)
    {
        AccountId accountId = accountClient.getAccountId();
        UserId userId = accountClient.getAdminUserId();
        RoleId adminRoleId = accountClient.getAdminRoleId();
        Map<RoleName, RoleId> roles = client.listEnabledRoles(accountClient.createDispatchSession(userId, adminRoleId));
        RoleId roleId = requireNonNull(roles.get(new RoleName(roleName)), "roles.get(roleName) is null");
        String principal = createPrincipalString(accountId, userId, roleId);
        Set<String> enabledRoles = client.listEnabledRoles(accountClient.createDispatchSession(userId, roleId)).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());
        return new Identity.Builder(accountClient.getAdminEmail())
                .withPrincipal(new BasicPrincipal(principal))
                .withEnabledRoles(enabledRoles)
                .build();
    }

    public Identity identity(TestUser testUser, RoleId roleId)
    {
        return identity(testUser.getEmail(), testUser.getUserId(), roleId);
    }

    public Identity identity(UserId userId, RoleId roleId)
    {
        return identity(accountClient.getAdminEmail(), userId, roleId);
    }

    public Identity identity(String email, UserId userId, RoleId roleId)
    {
        DispatchSession dispatchSession = dispatchSessionCache.getUnchecked(new UserIdAndRoleId(userId, roleId));

        Set<String> enabledRoles = client.listEnabledRoles(dispatchSession).keySet().stream()
                .map(RoleName::getName)
                .collect(toImmutableSet());

        return createIdentity(
                email,
                dispatchSession.getAccountId(),
                dispatchSession.getUserId(),
                dispatchSession.getRoleId(),
                enabledRoles,
                dispatchSession.getAccessToken(),
                GalaxyIdentityType.PORTAL);
    }

    public Session session(UserId userId, RoleId roleId)
    {
        return TestingSession.testSessionBuilder()
                .setIdentity(identity(userId, roleId))
                .setQueryId(new QueryId(String.valueOf(queryIds.incrementAndGet())))
                .build();
    }

    public Session session(TestUser testUser, RoleId roleId)
    {
        return TestingSession.testSessionBuilder()
                .setIdentity(identity(testUser, roleId))
                .setQueryId(new QueryId(String.valueOf(queryIds.incrementAndGet())))
                .build();
    }

    public Session session(RoleId roleId)
    {
        return withNewQueryId(session(accountClient.getAdminUserId(), roleId));
    }

    public Session adminSession()
    {
        return session(accountClient.getAdminRoleId());
    }

    public Session publicSession()
    {
        return session(accountClient.getPublicRoleId());
    }

    public static Session withNewTransactionId(Session session)
    {
        return Session.builder(session).setIdentity(Identity.from(session.getIdentity())
                        .withAdditionalExtraCredentials(ImmutableMap.of(TRANSACTION_ID_KEY, TransactionId.create().toString()))
                        .build())
                .build();
    }

    public SystemSecurityContext context(Session session)
    {
        return new SystemSecurityContext(session.getIdentity(), new QueryId(String.valueOf(queryIds.incrementAndGet())), Instant.now());
    }

    public SystemSecurityContext adminContext()
    {
        return withNewQueryId(context(session(accountClient.getAdminRoleId())));
    }

    public SystemSecurityContext publicContext()
    {
        return withNewQueryId(context(session(accountClient.getPublicRoleId())));
    }

    private Session withNewQueryId(Session session)
    {
        return TestingSession.testSessionBuilder()
                .setIdentity(session.getIdentity())
                .setQueryId(new QueryId(String.valueOf(queryIds.incrementAndGet())))
                .build();
    }

    private SystemSecurityContext withNewQueryId(SystemSecurityContext context)
    {
        return new SystemSecurityContext(context.getIdentity(), new QueryId(String.valueOf(queryIds.incrementAndGet())), Instant.now());
    }

    public String getAnyCatalogName()
    {
        return catalogResolver.getCatalogNames().stream()
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Could not find a catalog name"));
    }

    private record UserIdAndRoleId(UserId userId, RoleId roleId)
    {
        private UserIdAndRoleId
        {
            requireNonNull(userId, "userId is null");
            requireNonNull(roleId, "roleId is nul");
        }
    }
}
