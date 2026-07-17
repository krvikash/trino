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

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.cache.CacheKeyAndResult;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeOperation.GetEntityPrivileges;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeResult;
import io.starburst.stargate.accesscontrol.cache.TrinoPrivilegeResult.GetEntityPrivilegesResult;
import io.starburst.stargate.accesscontrol.cache.TrinoSecurityCacheKey;
import io.starburst.stargate.accesscontrol.cache.UserIdAndRoleId;
import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.TrinoSecurityApi;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.accesscontrol.privilege.GrantKind;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.id.UserId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.server.starburst.accesscontrol.GalaxyAccountPermissionsCache.CacheKeyAndResultInfo;
import io.trino.server.starburst.security.GalaxyTestHelper;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.starburst.stargate.accesscontrol.client.ContentsVisibility.ALLOW_ALL;
import static io.starburst.stargate.accesscontrol.client.ContentsVisibility.DENY_ALL;
import static io.trino.server.starburst.accesscontrol.MetadataAccessControllerSupplier.extractTransactionId;
import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static io.trino.server.starburst.security.GalaxyTestHelper.FEARLESS_LEADER;
import static io.trino.server.starburst.security.GalaxyTestHelper.PUBLIC;
import static io.trino.server.starburst.security.GalaxyTestHelper.withNewTransactionId;
import static io.trino.spi.security.Privilege.SELECT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGalaxyAccountPermissionsCache
{
    private GalaxyTestHelper helper;
    private UserId userId;
    private RoleId adminRoleId;
    private RoleId publicRoleId;
    private TrinoSecurityApi trinoSecurityApi;
    private GalaxySecurityMetadata metadataApi;

    @BeforeAll
    public void initialize()
            throws Exception
    {
        helper = new GalaxyTestHelper();
        helper.initialize();
        userId = helper.getAccountClient().getAdminUserId();
        adminRoleId = helper.getAccountClient().getAdminRoleId();
        publicRoleId = helper.getAccountClient().getPublicRoleId();
        trinoSecurityApi = helper.getClient();
        metadataApi = helper.getMetadataApi();
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (helper != null) {
            helper.close();
        }
        helper = null;
        metadataApi = null;
    }

    @Test
    public void testAccountPermissions()
    {
        GalaxyAccountPermissionsCache cache = new GalaxyAccountPermissionsCache(Duration.ofMinutes(1), helper.getCatalogResolver(), trinoSecurityApi, GalaxySystemAccessControlConfig.AccessControlMode.GALAXY);
        String catalogName = helper.getAnyCatalogName();
        Session adminSession = withNewTransactionId(helper.adminSession());
        DispatchSession adminDispatchSession = toDispatchSession(adminSession);
        TransactionId transactionId = extractTransactionId(adminSession.getIdentity()).orElseThrow();
        CatalogId catalogId = helper.getCatalogId(catalogName);
        TableId tableId = new TableId(catalogId, "sf1", "region");

        // The list of cache references for the queryId starts out empty
        assertThat(cache.getTransactionCacheReferences(transactionId)).isEmpty();
        EntityPrivileges privilegesFromApi = trinoSecurityApi.getEntityPrivileges(adminDispatchSession, tableId);
        EntityPrivileges privilegesFromCache = cache.getEntityPrivileges(transactionId, adminDispatchSession, adminRoleId, tableId, false);
        assertThat(privilegesFromApi).isEqualTo(privilegesFromCache);

        // The list of cache references for the queryId is still empty
        assertThat(cache.getTransactionCacheReferences(transactionId)).isEmpty();

        // Creating a new adminSession object gives us a new queryId
        Session newAdminSession = withNewTransactionId(helper.adminSession());
        TransactionId newTransactionId = extractTransactionId(newAdminSession.getIdentity()).orElseThrow();

        // No cache references for the new queryId either
        assertThat(cache.getTransactionCacheReferences(newTransactionId)).isEmpty();

        // If the privileges are fetched by a different queryId, the privileges come from the cache
        assertThat(privilegesFromApi).isEqualTo(cache.getEntityPrivileges(newTransactionId, adminDispatchSession, adminRoleId, tableId, false));
        List<CacheKeyAndResultInfo> queryCacheReferences = cache.getTransactionCacheReferences(newTransactionId);
        assertThat(queryCacheReferences.size()).isEqualTo(1);
        CacheKeyAndResultInfo cacheKeyAndResult = queryCacheReferences.get(0);
        assertThat(cacheKeyAndResult.cacheKey().userIdAndRoleId()).isEqualTo(new UserIdAndRoleId(userId, adminRoleId));
        assertThat(cacheKeyAndResult.cacheKey().operation()).isEqualTo(new GetEntityPrivileges(tableId, false));
        assertThat(cacheKeyAndResult.result().result()).isEqualTo(new GetEntityPrivilegesResult(privilegesFromApi));

        // Grant SELECT on catalogName.sf1.region to fearless leader
        metadataApi.grantTablePrivileges(
                adminSession,
                new QualifiedObjectName(catalogName, "sf1", "region"),
                ImmutableSet.of(SELECT),
                new TrinoPrincipal(PrincipalType.ROLE, FEARLESS_LEADER),
                true);

        newAdminSession = withNewTransactionId(helper.adminSession());
        newTransactionId = extractTransactionId(newAdminSession.getIdentity()).orElseThrow();
        List<CacheKeyAndResultInfo> cacheReferencesAfterGrant = cache.getTransactionCacheReferences(newTransactionId);
        assertThat(cacheReferencesAfterGrant).isEmpty();
        EntityPrivileges privilegesAfterGrant = trinoSecurityApi.getEntityPrivileges(adminDispatchSession, tableId);
        assertThat(privilegesAfterGrant).isNotEqualTo(privilegesFromApi);
        privilegesFromCache = cache.getEntityPrivileges(transactionId, adminDispatchSession, adminRoleId, tableId, false);
        assertThat(privilegesFromCache).isNotEqualTo(privilegesAfterGrant);
    }

    @Test
    public void testAccountPermissionsVisibility()
    {
        GalaxyAccountPermissionsCache cache = new GalaxyAccountPermissionsCache(Duration.ofMinutes(1), helper.getCatalogResolver(), trinoSecurityApi, GalaxySystemAccessControlConfig.AccessControlMode.GALAXY);
        Iterator<String> catalogIterator = helper.getCatalogResolver().getCatalogNames().iterator();
        String catalog1 = catalogIterator.next();
        String catalog2 = catalogIterator.next();
        CatalogId catalogId1 = helper.getCatalogId(catalog1);
        CatalogId catalogId2 = helper.getCatalogId(catalog2);
        String schema1 = "sf1";
        String schema2 = "sf100";

        Session adminSession = withNewTransactionId(helper.adminSession());
        Session publicSession = withNewTransactionId(helper.publicSession());
        DispatchSession adminDispatchSession = toDispatchSession(adminSession);
        DispatchSession publicDispatchSession = toDispatchSession(publicSession);
        TransactionId adminTransactionId = extractTransactionId(adminSession.getIdentity()).orElseThrow();
        TransactionId publicTransactionId = extractTransactionId(publicSession.getIdentity()).orElseThrow();
        TableId catalog1Schema1Table1 = new TableId(catalogId1, schema1, "region");
        TableId catalog1Schema1Table2 = new TableId(catalogId1, schema1, "customer");
        TableId catalog1Schema2Table1 = new TableId(catalogId1, schema2, catalog1Schema1Table1.getTableName());
        TableId catalog2Schema1Table1 = new TableId(catalogId2, schema1, catalog1Schema1Table1.getTableName());
        TableId catalog2Schema1Table2 = new TableId(catalogId2, schema1, "customer");
        TableId catalog2Schema2Table1 = new TableId(catalogId2, schema2, catalog1Schema1Table1.getTableName());

        // Initially, admin should see all tables, schemas and catalogs, while public should see none
        assertThat(cache.getCatalogVisibility(adminTransactionId, adminDispatchSession, ImmutableSet.of(catalogId1)))
                .isEqualTo(ALLOW_ALL);
        assertThat(cache.getCatalogVisibility(publicTransactionId, publicDispatchSession, ImmutableSet.of(catalogId1)))
                .isEqualTo(DENY_ALL);

        assertThat(cache.getVisibilityForSchemas(adminTransactionId, adminDispatchSession, catalogId1, ImmutableSet.of(schema1)))
                .isEqualTo(ALLOW_ALL);
        assertThat(cache.getVisibilityForSchemas(publicTransactionId, publicDispatchSession, catalogId1, ImmutableSet.of(schema1)))
                .isEqualTo(DENY_ALL);

        assertThat(cache.getVisibilityForTables(adminTransactionId, adminDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName())))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalog1Schema1Table1.getTableName())));
        assertThat(cache.getVisibilityForTables(publicTransactionId, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName())))
                .isEqualTo(DENY_ALL);

        // The list of cache references for the queryId is still empty as nothing was loaded from it
        assertThat(cache.getTransactionCacheReferences(adminTransactionId)).isEmpty();
        assertThat(cache.getTransactionCacheReferences(publicTransactionId)).isEmpty();

        // Create new sessions and query over a wider set of catalogs/schemas/tables to test the case where at least one result is in the cache
        TransactionId anotherAdminTransaction = extractTransactionId(withNewTransactionId(helper.adminSession()).getIdentity()).orElseThrow();
        TransactionId anotherPublicTransaction = extractTransactionId(withNewTransactionId(helper.publicSession()).getIdentity()).orElseThrow();
        assertThat(cache.getCatalogVisibility(anotherAdminTransaction, adminDispatchSession, ImmutableSet.of(catalogId1, catalogId2)))
                .isEqualTo(ALLOW_ALL);
        assertThat(cache.getCatalogVisibility(anotherPublicTransaction, publicDispatchSession, ImmutableSet.of(catalogId1, catalogId2)))
                .isEqualTo(DENY_ALL);

        assertThat(cache.getVisibilityForSchemas(anotherAdminTransaction, adminDispatchSession, catalogId1, ImmutableSet.of(schema1, schema2)))
                .isEqualTo(ALLOW_ALL);
        assertThat(cache.getVisibilityForSchemas(anotherPublicTransaction, publicDispatchSession, catalogId1, ImmutableSet.of(schema1, schema2)))
                .isEqualTo(DENY_ALL);

        assertThat(cache.getVisibilityForTables(anotherAdminTransaction, adminDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName(), catalog1Schema1Table2.getTableName())))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalog1Schema1Table1.getTableName(), catalog1Schema1Table2.getTableName())));
        assertThat(cache.getVisibilityForTables(anotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName(), catalog1Schema1Table2.getTableName())))
                .isEqualTo(DENY_ALL);

        // There were no items that were able to use the cache
        assertThat(cache.getTransactionCacheReferences(anotherAdminTransaction)).isEmpty();
        assertThat(cache.getTransactionCacheReferences(anotherPublicTransaction)).isEmpty();

        // Grant a privilege on one of the tables to public
        // This will cause a mismatch between what's in the cache and what's the source of truth
        metadataApi.grantTablePrivileges(
                adminSession,
                new QualifiedObjectName(catalog1, schema1, catalog1Schema1Table1.getTableName()),
                ImmutableSet.of(SELECT),
                new TrinoPrincipal(PrincipalType.ROLE, PUBLIC),
                false);
        assertThat(cache.getCatalogVisibility(anotherPublicTransaction, publicDispatchSession, ImmutableSet.of(catalogId1, catalogId2)))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForSchemas(anotherPublicTransaction, publicDispatchSession, catalogId1, ImmutableSet.of(schema1, schema2)))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForTables(anotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName(), catalog1Schema1Table2.getTableName())))
                .isEqualTo(DENY_ALL);

        // But the cache is hit for catalog and schema visibility
        // Note: table visibility doesn't get hit because it's added to the cache only table-by-table
        assertThat(cache.getTransactionCacheReferences(anotherPublicTransaction))
                .hasSize(2)
                .map(info -> new CacheKeyAndResult(info.cacheKey(), info.result().result()))
                .containsExactlyInAnyOrder(new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetCatalogVisibility(Optional.of(ImmutableSet.of(catalogId1, catalogId2)))),
                                new TrinoPrivilegeResult.GetCatalogVisibilityResult(DENY_ALL)),
                        new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetVisibilityForSchemas(catalogId1, ImmutableSet.of(schema1, schema2))),
                                new TrinoPrivilegeResult.GetVisibilityForSchemasResult(DENY_ALL)));

        // Grant another privilege so the real source of truth is visibility for multiple tables
        metadataApi.grantTablePrivileges(
                adminSession,
                new QualifiedObjectName(catalog2, schema1, catalog2Schema1Table2.getTableName()),
                ImmutableSet.of(SELECT),
                new TrinoPrincipal(PrincipalType.ROLE, PUBLIC),
                false);

        // Get a new transaction ID for public, and test again, only with a single catalog
        // all should be 'found' in the cache
        TransactionId yetAnotherPublicTransaction = extractTransactionId(withNewTransactionId(helper.publicSession()).getIdentity()).orElseThrow();
        assertThat(cache.getCatalogVisibility(yetAnotherPublicTransaction, publicDispatchSession, ImmutableSet.of(catalogId1)))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getCatalogVisibility(yetAnotherPublicTransaction, publicDispatchSession, ImmutableSet.of(catalogId1, catalogId2)))
                .isEqualTo(DENY_ALL);
        // just catalog2 is not cached by itself
        assertThat(cache.getCatalogVisibility(yetAnotherPublicTransaction, publicDispatchSession, ImmutableSet.of(catalogId2)))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalogId2.getBaseEntityIdString())));

        // These schemaVisibility calls have been cached
        assertThat(cache.getVisibilityForSchemas(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, ImmutableSet.of(schema1)))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForSchemas(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, ImmutableSet.of(schema1, schema2)))
                .isEqualTo(DENY_ALL);

        // schema2 or schema for catalog2 was not cached
        assertThat(cache.getVisibilityForSchemas(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, ImmutableSet.of(schema2)))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForSchemas(yetAnotherPublicTransaction, publicDispatchSession, catalogId2, ImmutableSet.of(schema1)))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(schema1)));

        // No getVisibilityForTables calls are cached per table
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName(), catalog1Schema1Table2.getTableName())))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalog1Schema1Table1.getTableName())));
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table1.getTableName())))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalog1Schema1Table1.getTableName())));
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog1Schema1Table2.getTableName())))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema2, ImmutableSet.of(catalog1Schema2Table1.getTableName())))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId2, schema1, ImmutableSet.of(catalog2Schema1Table1.getTableName())))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog2Schema1Table2.getTableName())))
                .isEqualTo(DENY_ALL);
        assertThat(cache.getVisibilityForTables(yetAnotherPublicTransaction, publicDispatchSession, catalogId1, schema1, ImmutableSet.of(catalog2Schema2Table1.getTableName())))
                .isEqualTo(new ContentsVisibility(GrantKind.DENY, ImmutableSet.of(catalog2Schema2Table1.getTableName())));

        assertThat(cache.getTransactionCacheReferences(yetAnotherPublicTransaction))
                .hasSize(4)
                .map(info -> new CacheKeyAndResult(info.cacheKey(), info.result().result()))
                .containsExactlyInAnyOrder(new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetCatalogVisibility(Optional.of(ImmutableSet.of(catalogId1, catalogId2)))),
                                new TrinoPrivilegeResult.GetCatalogVisibilityResult(DENY_ALL)),
                        new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetCatalogVisibility(Optional.of(ImmutableSet.of(catalogId1)))),
                                new TrinoPrivilegeResult.GetCatalogVisibilityResult(DENY_ALL)),
                        new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetVisibilityForSchemas(catalogId1, ImmutableSet.of(schema1))),
                                new TrinoPrivilegeResult.GetVisibilityForSchemasResult(DENY_ALL)),
                        new CacheKeyAndResult(
                                new TrinoSecurityCacheKey(new UserIdAndRoleId(userId, publicRoleId), new TrinoPrivilegeOperation.GetVisibilityForSchemas(catalogId1, ImmutableSet.of(schema1, schema2))),
                                new TrinoPrivilegeResult.GetVisibilityForSchemasResult(DENY_ALL)));
    }

    @Test
    public void testAccountCacheExpiration()
    {
        GalaxyAccountPermissionsCache cache = new GalaxyAccountPermissionsCache(Duration.ofMillis(100), helper.getCatalogResolver(), trinoSecurityApi, GalaxySystemAccessControlConfig.AccessControlMode.GALAXY);
        String catalogName = helper.getAnyCatalogName();
        Session adminSession = withNewTransactionId(helper.adminSession());
        DispatchSession adminDispatchSession = toDispatchSession(adminSession);
        TransactionId transactionId = extractTransactionId(adminSession.getIdentity()).orElseThrow();
        CatalogId catalogId = helper.getCatalogId(catalogName);
        TableId tableId = new TableId(catalogId, "sf1", "region");

        // The list of cache references for the queryId starts out empty
        assertThat(cache.getTransactionCacheReferences(transactionId)).isEmpty();
        EntityPrivileges privilegesFromApi = trinoSecurityApi.getEntityPrivileges(adminDispatchSession, tableId);
        EntityPrivileges privilegesFromCache = cache.getEntityPrivileges(transactionId, adminDispatchSession, adminRoleId, tableId, false);
        assertThat(privilegesFromApi).isEqualTo(privilegesFromCache);

        // Sleep for 300ms and verify that the cache entry has expired
        try {
            Thread.sleep(300);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(cache.getTransactionCacheReferences(transactionId)).isEmpty();
    }

    record TransactionAndSession(DispatchSession session, TransactionId transactionId)
    {
        public TransactionAndSession
        {
            requireNonNull(session, "session is null");
            requireNonNull(transactionId, "transactionId is null");
        }
    }

    private TransactionAndSession newTransactionAndSession()
    {
        Session adminSession = withNewTransactionId(helper.adminSession());
        DispatchSession adminDispatchSession = toDispatchSession(adminSession);
        TransactionId transactionId = extractTransactionId(adminSession.getIdentity()).orElseThrow();
        return new TransactionAndSession(adminDispatchSession, transactionId);
    }

    @Test
    public void testValidationNotAppliedAfterTransactionTime()
    {
        GalaxyAccountPermissionsCache cache = new GalaxyAccountPermissionsCache(Duration.ofMinutes(1), helper.getCatalogResolver(), trinoSecurityApi, GalaxySystemAccessControlConfig.AccessControlMode.GALAXY);
        String catalogName = helper.getAnyCatalogName();

        CatalogId catalogId = helper.getCatalogId(catalogName);
        TableId tableId = new TableId(catalogId, "sf1", "region");

        // make two transaction IDs, for the sake of testing that a cache reference
        // fetched since the start of each transaction won't validate the result
        TransactionAndSession firstTransactionAndSession = newTransactionAndSession();
        cache.setTransactionIdInAnalysis(firstTransactionAndSession.transactionId(), true);
        TransactionAndSession secondTransactionAndSession = newTransactionAndSession();
        cache.setTransactionIdInAnalysis(secondTransactionAndSession.transactionId(), true);

        // The list of cache references for each query starts out empty
        assertThat(cache.getTransactionCacheReferences(firstTransactionAndSession.transactionId())).isEmpty();
        assertThat(cache.getTransactionCacheReferences(secondTransactionAndSession.transactionId())).isEmpty();

        // make sure the call works correctly, and fetch from the cache, so the result is now populated in the cache
        EntityPrivileges privilegesFromApi = trinoSecurityApi.getEntityPrivileges(firstTransactionAndSession.session(), tableId);
        EntityPrivileges privilegesFromCache = cache.getEntityPrivileges(firstTransactionAndSession.transactionId(), firstTransactionAndSession.session(), adminRoleId, tableId, false);
        assertThat(privilegesFromApi).isEqualTo(privilegesFromCache);

        // The list of cache references for the transactionId is still empty, but should be placed in the cache
        assertThat(cache.getTransactionCacheReferences(firstTransactionAndSession.transactionId())).isEmpty();
        assertThat(cache.getTransactionCacheReferences(secondTransactionAndSession.transactionId())).isEmpty();

        // fetch privileges again for both transactions, they should both come from the cache
        AtomicInteger totalRequestsFromCacheNotValidated = new AtomicInteger();
        ImmutableSet.of(firstTransactionAndSession, secondTransactionAndSession).forEach(transactionAndSession -> {
            assertThat(privilegesFromApi).isEqualTo(cache.getEntityPrivileges(transactionAndSession.transactionId(), transactionAndSession.session(), adminRoleId, tableId, false));
            List<CacheKeyAndResultInfo> queryCacheReferences = cache.getTransactionCacheReferences(transactionAndSession.transactionId());
            assertThat(queryCacheReferences.size()).isEqualTo(1);
            CacheKeyAndResultInfo cacheKeyAndResult = queryCacheReferences.get(0);
            assertThat(cacheKeyAndResult.cacheKey().userIdAndRoleId()).isEqualTo(new UserIdAndRoleId(userId, adminRoleId));
            assertThat(cacheKeyAndResult.cacheKey().operation()).isEqualTo(new GetEntityPrivileges(tableId, false));
            assertThat(cacheKeyAndResult.result().result()).isEqualTo(new GetEntityPrivilegesResult(privilegesFromApi));

            // even though the requests are served from the cache, validation *should not have happened*
            // because the results were initially retrieved after the start of the transaction
            cache.validateCachedResultsAndApplyUpdates(transactionAndSession.transactionId(), transactionAndSession.session(), queryCacheReferences, false);
            assertThat(cache.getValidationStatistics().getValidButNotValidatedCount())
                    .isEqualTo(totalRequestsFromCacheNotValidated.incrementAndGet());
        });

        // make sure no validation calls occurred
        assertThat(cache.getValidationStatistics().getResultValidationCalls()).isEqualTo(0);
        assertThat(cache.getValidationStatistics().getValidResultCount()).isEqualTo(0);

        // create another session, so the cached result will be validated
        TransactionAndSession thirdTransactionAndSession = newTransactionAndSession();
        cache.setTransactionIdInAnalysis(thirdTransactionAndSession.transactionId(), true);

        // This should come from the cache but need to be validated
        assertThat(privilegesFromApi).isEqualTo(cache.getEntityPrivileges(thirdTransactionAndSession.transactionId(), thirdTransactionAndSession.session(), adminRoleId, tableId, false));

        // Check that it's from the cache...
        List<CacheKeyAndResultInfo> thirdCacheReferences = cache.getTransactionCacheReferences(thirdTransactionAndSession.transactionId());
        assertThat(thirdCacheReferences.size()).isEqualTo(1);
        CacheKeyAndResultInfo cacheKeyAndResult = thirdCacheReferences.get(0);
        assertThat(cacheKeyAndResult.cacheKey().userIdAndRoleId()).isEqualTo(new UserIdAndRoleId(userId, adminRoleId));
        assertThat(cacheKeyAndResult.cacheKey().operation()).isEqualTo(new GetEntityPrivileges(tableId, false));
        assertThat(cacheKeyAndResult.result().result()).isEqualTo(new GetEntityPrivilegesResult(privilegesFromApi));
        cache.validateCachedResultsAndApplyUpdates(thirdTransactionAndSession.transactionId(), thirdTransactionAndSession.session(), thirdCacheReferences, false);

        // but it was validated
        assertThat(cache.getValidationStatistics().getValidButNotValidatedCount()).isEqualTo(totalRequestsFromCacheNotValidated.get());
        assertThat(cache.getValidationStatistics().getResultValidationCalls()).isEqualTo(1);
        assertThat(cache.getValidationStatistics().getValidResultCount()).isEqualTo(1);
    }
}
