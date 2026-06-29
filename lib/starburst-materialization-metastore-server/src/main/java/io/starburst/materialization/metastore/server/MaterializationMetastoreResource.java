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
import io.trino.spi.connector.CatalogSchemaTableName;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * Routing and behavior for the materialization metastore endpoints. Abstract on purpose: a
 * deployment registers a concrete subclass so it can declare its own authentication posture.
 * The JAX-RS routing annotations here are inherited by the registered subclass.
 */
@Produces(APPLICATION_JSON)
@Path("/v1/{metastoreId}/materializations")
public abstract class MaterializationMetastoreResource
{
    private final DbRawMaterializationMetastore metastore;

    protected MaterializationMetastoreResource(DbRawMaterializationMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @GET
    public List<RawMaterializationDefinition> list(@PathParam("metastoreId") MetastoreId metastoreId)
    {
        return metastore.listMaterializations(metastoreId);
    }

    @PUT
    @Consumes(APPLICATION_JSON)
    public Response createOrReplace(@PathParam("metastoreId") MetastoreId metastoreId, RawMaterializationDefinition definition)
    {
        metastore.createOrReplace(metastoreId, definition);
        return Response.ok().build();
    }

    @DELETE
    @Path("{catalogName}/{schemaName}/{materializedViewName}")
    public Response remove(
            @PathParam("metastoreId") MetastoreId metastoreId,
            @PathParam("catalogName") String catalogName,
            @PathParam("schemaName") String schemaName,
            @PathParam("materializedViewName") String materializedViewName)
    {
        metastore.remove(metastoreId, new CatalogSchemaTableName(catalogName, schemaName, materializedViewName));
        return Response.ok().build();
    }

    @POST
    @Path("rename")
    @Consumes(APPLICATION_JSON)
    public Response rename(@PathParam("metastoreId") MetastoreId metastoreId, RenameRequest request)
    {
        metastore.renameIfExists(metastoreId, request.source(), request.target(), request.targetStorageTableId());
        return Response.ok().build();
    }
}
