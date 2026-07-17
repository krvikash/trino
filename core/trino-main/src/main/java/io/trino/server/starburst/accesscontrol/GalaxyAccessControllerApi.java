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

import io.starburst.stargate.accesscontrol.client.ContentsVisibility;
import io.starburst.stargate.accesscontrol.client.GalaxyLanguageFunctionDetails;
import io.starburst.stargate.accesscontrol.privilege.EntityPrivileges;
import io.starburst.stargate.id.AccountId;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.EntityId;
import io.starburst.stargate.id.FunctionId;
import io.starburst.stargate.id.RoleId;
import io.starburst.stargate.id.RoleName;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.starburst.stargate.id.TableId;
import io.starburst.stargate.identity.DispatchSession;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.ViewExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public interface GalaxyAccessControllerApi
{
    Optional<CatalogId> getCatalogId(String catalogName);

    boolean isReadOnlyCatalog(String catalogName);

    Optional<SharedSchemaNameAndAccepted> getSharedCatalogSchemaName(String catalogName);

    EntityPrivileges getEntityPrivileges(SystemSecurityContext context, EntityId entity);

    EntityPrivileges getEntityPrivileges(Identity identity, EntityId entity);

    Map<RoleName, RoleId> listEnabledRoles(Identity identity);

    Map<RoleName, RoleId> listRoles(Identity identity);

    Map<RoleName, RoleId> listEnabledRoles(Identity identity, Function<Identity, DispatchSession> sessionCreator);

    Predicate<String> getCatalogVisibility(SystemSecurityContext context, Set<String> requestedCatalogs);

    void implyCatalogVisibility(SystemSecurityContext context, String catalogName);

    boolean hasImpliedCatalogVisibility(SystemSecurityContext context, String catalogName);

    AccountId getAccountId(Identity identity);

    Predicate<String> getVisibilityForSchemas(SystemSecurityContext context, CatalogId catalogId, Set<String> schemaNames);

    ContentsVisibility getVisibilityForTables(SystemSecurityContext context, CatalogId catalogId, String schemaName, Set<String> tableNames);

    String getRoleDisplayName(Identity identity, RoleId roleId);

    boolean shouldFilterOtherUsers();

    boolean canUseLocation(SystemSecurityContext context, String location);

    boolean canExecuteFunction(SystemSecurityContext context, FunctionId functionId);

    boolean isUserDefinedFunctionVisible(SystemSecurityContext context, FunctionId functionId);

    List<ViewExpression> getRowFilters(SystemSecurityContext context, TableId tableId);

    Optional<ViewExpression> getColumnMask(SystemSecurityContext context, ColumnSchema column, TableId tableId);

    Set<GalaxyLanguageFunctionDetails> getAvailableFunctions(SystemSecurityContext context);

    Optional<Boolean> isLiveTableStopped(SystemSecurityContext context, TableId tableId);

    boolean isGalaxyEntityPrivilegesEnabled();

    Set<SchemaTableName> getAlwaysVisibleSystemTables(String catalogName);
}
