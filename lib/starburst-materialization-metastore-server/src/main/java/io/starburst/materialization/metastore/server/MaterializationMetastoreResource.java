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

import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.client.RenameRequest;
import io.starburst.stargate.id.AccountId;
import io.trino.spi.connector.CatalogSchemaTableName;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.util.List;

import static io.starburst.materialization.metastore.client.HttpMaterializationMetastore.ACCOUNT_ID;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Routing and behavior for the materialization metastore endpoints. Abstract on purpose: a
 * deployment registers a concrete subclass so it can declare its own authentication posture.
 * The JAX-RS routing annotations here are inherited by the registered subclass.
 */
@Produces(APPLICATION_JSON)
@Path("materializations/v1/{metastoreId}")
public abstract class MaterializationMetastoreResource
{
    private final DbRawMaterializationMetastore metastore;
    private final MaterializationAccountResolver accountResolver;

    protected MaterializationMetastoreResource(DbRawMaterializationMetastore metastore, MaterializationAccountResolver accountResolver)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.accountResolver = requireNonNull(accountResolver, "accountResolver is null");
    }

    @GET
    public List<RawMaterializationDefinition> list(
            @PathParam("metastoreId") MetastoreId metastoreId,
            @HeaderParam(ACCOUNT_ID) String requestAccountId)
    {
        return metastore.listMaterializations(metastoreId, accountId(metastoreId, requestAccountId));
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    public Response createOrReplace(
            @PathParam("metastoreId") MetastoreId metastoreId,
            @HeaderParam(ACCOUNT_ID) String requestAccountId,
            RawMaterializationDefinition definition)
    {
        metastore.createOrReplace(metastoreId, accountId(metastoreId, requestAccountId), definition);
        return Response.ok().build();
    }

    @DELETE
    @Path("{catalogName}/{schemaName}/{materializedViewName}")
    public Response remove(
            @PathParam("metastoreId") MetastoreId metastoreId,
            @HeaderParam(ACCOUNT_ID) String requestAccountId,
            @PathParam("catalogName") String catalogName,
            @PathParam("schemaName") String schemaName,
            @PathParam("materializedViewName") String materializedViewName)
    {
        metastore.remove(metastoreId, accountId(metastoreId, requestAccountId), new CatalogSchemaTableName(catalogName, schemaName, materializedViewName));
        return Response.ok().build();
    }

    @POST
    @Path("rename")
    @Consumes(APPLICATION_JSON)
    public Response rename(
            @PathParam("metastoreId") MetastoreId metastoreId,
            @HeaderParam(ACCOUNT_ID) String requestAccountId,
            RenameRequest request)
    {
        metastore.renameIfExists(metastoreId, accountId(metastoreId, requestAccountId), request.source(), request.target(), request.targetStorageTableId());
        return Response.ok().build();
    }

    /**
     * The owning account always comes from the resolver. Clients predating account ids don't send
     * one; a client that does must name the owning account, which catches a cluster configured
     * with another account's metastore.
     */
    private AccountId accountId(MetastoreId metastoreId, @Nullable String requestAccountId)
    {
        AccountId accountId = accountResolver.accountId(metastoreId);
        if (requestAccountId != null && !requestAccountId.equals(accountId.toString())) {
            throw new ForbiddenException("Metastore %s is not owned by account %s".formatted(metastoreId, requestAccountId));
        }
        return accountId;
    }
}
